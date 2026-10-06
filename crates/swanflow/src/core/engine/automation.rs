//! ADR-014 Stage 3 — Automation conditions DSL (stateless evaluator).
//!
//! duckOrch-style DSL evaluated on every daemon sensor tick:
//!
//! ```text
//! eager
//! on_cron("*/5 * * * *")
//! on_interval(30m)
//! freshness_violated(5m)
//! eager & !freshness_violated(1h)
//! on_cron("0 * * * *") | freshness_violated(10m)
//! ```
//!
//! Precedence: `!` > `&` > `|`; parentheses group. The evaluator is
//! **stateless**: every decision is derived from the (now, last_run,
//! last_eval) triple handed in by the caller — no hidden timer state, so
//! the truth table is unit-testable and a daemon restart loses nothing.

use chrono::{DateTime, Datelike, Duration, Timelike, Utc};

// ---------------------------------------------------------------------------
// AST
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AutomationCondition {
    /// Always ready.
    Eager,
    /// Ready when a cron fire time falls in (last_eval, now].
    OnCron(CronExpr),
    /// Ready when now - last fire (run or eval) >= the interval.
    OnInterval { secs: u64 },
    /// Ready when the asset's last materialization is older than max_lag
    /// (or it never materialized).
    FreshnessViolated { max_lag_secs: u64 },
    And(Box<AutomationCondition>, Box<AutomationCondition>),
    Or(Box<AutomationCondition>, Box<AutomationCondition>),
    Not(Box<AutomationCondition>),
}

// ---------------------------------------------------------------------------
// Duration literal: 30s / 5m / 2h / 1d, combinable: 1h30m
// ---------------------------------------------------------------------------

pub fn parse_duration(s: &str) -> Result<u64, String> {
    let mut total: u64 = 0;
    let mut num = String::new();
    let mut matched = false;
    for ch in s.chars() {
        if ch.is_ascii_digit() {
            num.push(ch);
        } else {
            let n: u64 = num
                .parse()
                .map_err(|_| format!("invalid number in duration '{s}'"))?;
            num.clear();
            let mult = match ch {
                's' => 1,
                'm' => 60,
                'h' => 3600,
                'd' => 86400,
                _ => return Err(format!("unknown unit '{ch}' in duration '{s}'")),
            };
            total = total
                .checked_add(n.checked_mul(mult).ok_or_else(|| format!("overflow in '{s}'"))?)
                .ok_or_else(|| format!("overflow in '{s}'"))?;
            matched = true;
        }
    }
    if !num.is_empty() || !matched {
        return Err(format!("duration '{s}' must end in a unit (s/m/h/d)"));
    }
    if total == 0 {
        return Err(format!("duration '{s}' must be > 0 (0 would fire every tick)"));
    }
    // Consumers cast secs to i64 for chrono Duration — reject anything that
    // would wrap (chrono's practical bound is i64::MAX milliseconds).
    if total > i64::MAX as u64 / 1000 {
        return Err(format!("duration '{s}' exceeds the representable range"));
    }
    Ok(total)
}

// ---------------------------------------------------------------------------
// Minimal 5-field cron: minute hour day-of-month month day-of-week
// Supports: *  */n  a-b  a-b/n  lists (a,b,c)  single values
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CronExpr {
    minutes: Vec<u32>,
    hours: Vec<u32>,
    doms: Vec<u32>,
    months: Vec<u32>,
    dows: Vec<u32>,
    dom_star: bool,
    dow_star: bool,
    raw: String,
}

fn parse_field(field: &str, lo: u32, hi: u32, name: &str) -> Result<Vec<u32>, String> {
    let mut out = Vec::new();
    for part in field.split(',') {
        let (range, step) = match part.split_once('/') {
            Some((r, s)) => (r, s.parse::<u32>().map_err(|_| format!("bad step in '{part}'"))?),
            None => (part, 1),
        };
        if step == 0 {
            return Err(format!("step 0 in '{part}'"));
        }
        let (a, b) = if range == "*" {
            (lo, hi)
        } else if let Some((a, b)) = range.split_once('-') {
            (
                a.parse::<u32>().map_err(|_| format!("bad range '{range}'"))?,
                b.parse::<u32>().map_err(|_| format!("bad range '{range}'"))?,
            )
        } else {
            let v = range
                .parse::<u32>()
                .map_err(|_| format!("bad value '{range}' in {name}"))?;
            (v, v)
        };
        if a < lo || b > hi || a > b {
            return Err(format!("{name} range {a}-{b} out of {lo}-{hi}"));
        }
        let mut v = a;
        while v <= b {
            if !out.contains(&v) {
                out.push(v);
            }
            v += step;
        }
    }
    if out.is_empty() {
        return Err(format!("{name} field matched nothing"));
    }
    out.sort_unstable();
    Ok(out)
}

impl CronExpr {
    pub fn parse(s: &str) -> Result<Self, String> {
        let fields: Vec<&str> = s.split_whitespace().collect();
        if fields.len() != 5 {
            return Err(format!("cron '{s}' must have 5 fields (min hour dom mon dow)"));
        }
        let dom_star = fields[2] == "*";
        let dow_star = fields[4] == "*";
        Ok(Self {
            minutes: parse_field(fields[0], 0, 59, "minute")?,
            hours: parse_field(fields[1], 0, 23, "hour")?,
            doms: parse_field(fields[2], 1, 31, "day-of-month")?,
            months: parse_field(fields[3], 1, 12, "month")?,
            dows: parse_field(fields[4], 0, 6, "day-of-week")?,
            dom_star,
            dow_star,
            raw: s.to_string(),
        })
    }

    fn matches(&self, t: &DateTime<Utc>) -> bool {
        if !self.months.contains(&t.month()) {
            return false;
        }
        if !self.hours.contains(&t.hour()) {
            return false;
        }
        if !self.minutes.contains(&t.minute()) {
            return false;
        }
        // Vixie-cron semantics: if both dom and dow are restricted, either
        // may match; if only one is restricted, that one must match.
        let dom_ok = self.doms.contains(&t.day());
        // chrono: Sunday = 7 in %u numbering... Weekday::num_days_from_monday
        // gives Mon=0..Sun=6 — cron's 0=Sunday maps to (dow + 6) % 7 in this
        // numbering. We parse dow as 0=Sun..6=Sat, so convert:
        let cron_dow = (t.weekday().num_days_from_monday() + 1) % 7; // Mon=1..Sun=0? see below
        let _ = cron_dow; // replaced by direct map below
        let dow_idx = match t.weekday() {
            chrono::Weekday::Sun => 0,
            chrono::Weekday::Mon => 1,
            chrono::Weekday::Tue => 2,
            chrono::Weekday::Wed => 3,
            chrono::Weekday::Thu => 4,
            chrono::Weekday::Fri => 5,
            chrono::Weekday::Sat => 6,
        };
        let dow_ok = self.dows.contains(&dow_idx);
        match (self.dom_star, self.dow_star) {
            (false, false) => dom_ok || dow_ok,
            (false, true) => dom_ok,
            (true, false) => dow_ok,
            (true, true) => true,
        }
    }

    /// Earliest fire time strictly after `after`, scanning minute by minute.
    /// Bounded at 366 days ahead (returns None past the horizon).
    pub fn next_after(&self, after: DateTime<Utc>) -> Option<DateTime<Utc>> {
        let mut t = after + Duration::minutes(1);
        let horizon = after + Duration::days(366);
        while t <= horizon {
            if self.matches(&t) {
                return Some(t);
            }
            t += Duration::minutes(1);
        }
        None
    }

    /// Does any fire time fall in (last_eval, now]?
    pub fn fires_between(&self, last_eval: DateTime<Utc>, now: DateTime<Utc>) -> bool {
        if now <= last_eval {
            return false;
        }
        // Fast path: the current minute matches AND its canonical fire time
        // (second=0) is strictly after last_eval. Without the second check,
        // two ticks inside the SAME minute both match (e.g. 12:00:05 then
        // 12:00:35) and the 12:00 fire time is consumed twice.
        let now_min = now.with_second(0).unwrap().with_nanosecond(0).unwrap();
        if now_min > last_eval && self.matches(&now_min) {
            return true;
        }
        self.next_after(last_eval).map_or(false, |f| f <= now)
    }
}

impl std::fmt::Display for CronExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.raw)
    }
}

// ---------------------------------------------------------------------------
// Parser: ! > & > |, parentheses
// ---------------------------------------------------------------------------

struct Lexer<'a> {
    src: &'a [u8],
    pos: usize,
}

impl<'a> Lexer<'a> {
    fn tokens(src: &'a str) -> Result<Vec<String>, String> {
        let mut lx = Lexer { src: src.as_bytes(), pos: 0 };
        let mut out = Vec::new();
        while lx.pos < lx.src.len() {
            let c = lx.src[lx.pos] as char;
            match c {
                ' ' | '\t' => lx.pos += 1,
                '&' | '|' | '!' | '(' | ')' => {
                    out.push(c.to_string());
                    lx.pos += 1;
                }
                '"' => {
                    // Quoted argument (e.g. on_cron("*/5 * * * *")): the
                    // docs use this form; keep spaces inside the token.
                    lx.pos += 1;
                    let start = lx.pos;
                    while lx.pos < lx.src.len() && lx.src[lx.pos] != b'"' {
                        lx.pos += 1;
                    }
                    if lx.pos >= lx.src.len() {
                        return Err("unterminated quoted string".to_string());
                    }
                    let tok = std::str::from_utf8(&lx.src[start..lx.pos])
                        .map_err(|_| "invalid utf8".to_string())?
                        .to_string();
                    out.push(format!("\"{tok}\""));
                    lx.pos += 1; // closing quote
                }
                _ => {
                    let start = lx.pos;
                    while lx.pos < lx.src.len() {
                        let ch = lx.src[lx.pos] as char;
                        // Identifiers may contain letters, digits, ':', '_',
                        // '-', '.', and quoted cron strings keep spaces.
                        if ch.is_alphanumeric() || ":_-.*/,".contains(ch) {
                            lx.pos += 1;
                        } else {
                            break;
                        }
                    }
                    if lx.pos == start {
                        return Err(format!("unexpected char '{c}' at {}", lx.pos));
                    }
                    let tok = std::str::from_utf8(&lx.src[start..lx.pos])
                        .map_err(|_| "invalid utf8".to_string())?
                        .to_string();
                    out.push(tok);
                }
            }
        }
        Ok(out)
    }
}

pub fn parse_automation(s: &str) -> Result<AutomationCondition, String> {
    let tokens = Lexer::tokens(s)?;
    let mut parser = CondParser { tokens, pos: 0 };
    let cond = parser.parse_or()?;
    if parser.pos != parser.tokens.len() {
        return Err(format!("trailing tokens at {}", parser.tokens[parser.pos]));
    }
    Ok(cond)
}

struct CondParser {
    tokens: Vec<String>,
    pos: usize,
}

impl CondParser {
    fn peek(&self) -> Option<&str> {
        self.tokens.get(self.pos).map(|s| s.as_str())
    }

    fn bump(&mut self) -> Option<String> {
        let t = self.tokens.get(self.pos).cloned();
        if t.is_some() {
            self.pos += 1;
        }
        t
    }

    fn parse_or(&mut self) -> Result<AutomationCondition, String> {
        let mut left = self.parse_and()?;
        while self.peek() == Some("|") {
            self.bump();
            let right = self.parse_and()?;
            left = AutomationCondition::Or(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn parse_and(&mut self) -> Result<AutomationCondition, String> {
        let mut left = self.parse_not()?;
        while self.peek() == Some("&") {
            self.bump();
            let right = self.parse_not()?;
            left = AutomationCondition::And(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn parse_not(&mut self) -> Result<AutomationCondition, String> {
        if self.peek() == Some("!") {
            self.bump();
            let inner = self.parse_not()?;
            return Ok(AutomationCondition::Not(Box::new(inner)));
        }
        self.parse_atom()
    }

    fn parse_atom(&mut self) -> Result<AutomationCondition, String> {
        let tok = self.bump().ok_or("unexpected end of input")?;
        match tok.as_str() {
            "(" => {
                let inner = self.parse_or()?;
                if self.bump().as_deref() != Some(")") {
                    return Err("expected ')'".into());
                }
                Ok(inner)
            }
            "eager" => Ok(AutomationCondition::Eager),
            _ => {
                // func(arg) forms — lexer keeps '(' separate
                let arg = if self.peek() == Some("(") {
                    self.bump();
                    let mut inner = String::new();
                    // consume until matching ')' — allows spaces inside
                    // (cron expressions); handles nesting level for safety.
                    let mut depth = 1;
                    while let Some(c) = self.peek() {
                        if c == "(" {
                            depth += 1;
                        } else if c == ")" {
                            depth -= 1;
                            if depth == 0 {
                                break;
                            }
                        }
                        inner.push_str(&self.bump().unwrap());
                        inner.push(' ');
                    }
                    if self.bump().as_deref() != Some(")") {
                        return Err(format!("unclosed '(' after '{tok}'"));
                    }
                    inner.trim().to_string()
                } else {
                    return Err(format!("unknown atom '{tok}'"));
                };
                match tok.as_str() {
                    "on_cron" => Ok(AutomationCondition::OnCron(CronExpr::parse(&unquote(&arg))?)),
                    "on_interval" => Ok(AutomationCondition::OnInterval {
                        secs: parse_duration(&unquote(&arg))?,
                    }),
                    "freshness_violated" => Ok(AutomationCondition::FreshnessViolated {
                        max_lag_secs: parse_duration(&unquote(&arg))?,
                    }),
                    _ => Err(format!("unknown condition '{tok}'")),
                }
            }
        }
    }
}

/// Strip a balanced pair of surrounding double quotes, if present
/// (lexer preserves them inside the token).
fn unquote(s: &str) -> String {
    let t = s.trim();
    if t.len() >= 2 && t.starts_with('"') && t.ends_with('"') {
        t[1..t.len() - 1].to_string()
    } else {
        t.to_string()
    }
}

// ---------------------------------------------------------------------------
// Stateless evaluator
// ---------------------------------------------------------------------------

/// Everything the evaluator is allowed to know. Provided per (node, tick)
/// by the sensor loop; no state lives in this module.
#[derive(Debug, Clone)]
pub struct EvalContext {
    pub now: DateTime<Utc>,
    /// Last successful materialization of the asset (None = never).
    pub last_materialized_at: Option<DateTime<Utc>>,
    /// Last time this condition evaluated ready (run or no-op) — gates
    /// on_cron / on_interval re-fire.
    pub last_eval_ready_at: Option<DateTime<Utc>>,
    /// True when the asset ledger could not be read this tick (lock
    /// contention, corrupt file, …). Tri-state: `last_materialized_at ==
    /// None` + this flag distinguishes "never ran" from "can't tell".
    /// FreshnessViolated must NOT fire on unavailable — that would run
    /// workflows based on missing data; better to skip this tick and
    /// retry.
    pub ledger_unavailable: bool,
}

pub fn evaluate(cond: &AutomationCondition, ctx: &EvalContext) -> bool {
    match cond {
        AutomationCondition::Eager => true,
        AutomationCondition::OnCron(expr) => {
            let gate = ctx.last_eval_ready_at.unwrap_or(ctx.now - Duration::days(366));
            expr.fires_between(gate, ctx.now)
        }
        AutomationCondition::OnInterval { secs } => {
            let gate = ctx.last_eval_ready_at.unwrap_or(ctx.now - Duration::days(366));
            ctx.now - gate >= Duration::seconds(*secs as i64)
        }
        AutomationCondition::FreshnessViolated { max_lag_secs } => {
            if ctx.ledger_unavailable {
                // Can't tell freshness without the ledger — "never ran"
                // and "read failed" both surface as last_materialized_at
                // == None upstream. Firing here would treat every lock
                // hiccup as maximally stale. Skip; next tick retries.
                return false;
            }
            match ctx.last_materialized_at {
                None => true, // never materialized = maximally stale
                Some(t) => ctx.now - t >= Duration::seconds(*max_lag_secs as i64),
            }
        }
        AutomationCondition::And(a, b) => evaluate(a, ctx) && evaluate(b, ctx),
        AutomationCondition::Or(a, b) => evaluate(a, ctx) || evaluate(b, ctx),
        AutomationCondition::Not(a) => !evaluate(a, ctx),
    }
}

// ---------------------------------------------------------------------------
// Tests — the ADR-014 verification plan's "truth table"
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn at(s: &str) -> DateTime<Utc> {
        Utc.datetime_from_str(s, "%Y-%m-%d %H:%M:%S").unwrap()
    }

    fn ctx(now: &str, mat: Option<&str>, eval: Option<&str>) -> EvalContext {
        EvalContext {
            now: at(now),
            last_materialized_at: mat.map(at),
            last_eval_ready_at: eval.map(at),
            ledger_unavailable: false,
        }
    }

    #[test]
    fn freshness_violated_does_not_fire_when_ledger_unavailable() {
        // Tri-state: "never materialized" fires, "ledger unreadable" must
        // not — a lock hiccup or corrupt file is not evidence of staleness.
        let mut c = ctx("2026-10-06 12:00:00", None, None);
        assert!(
            evaluate(&AutomationCondition::FreshnessViolated { max_lag_secs: 300 }, &c),
            "never materialized (ledger readable) = maximally stale, fires"
        );
        c.ledger_unavailable = true;
        assert!(
            !evaluate(&AutomationCondition::FreshnessViolated { max_lag_secs: 300 }, &c),
            "ledger unavailable must NOT fire — skip this tick, retry next"
        );
        // With a known-good timestamp the flag is irrelevant (the read
        // succeeded), but a failed read of an existing row still must not.
        let mut c2 = ctx("2026-10-06 12:00:00", Some("2026-10-06 11:00:00"), None);
        c2.ledger_unavailable = true;
        assert!(
            !evaluate(&AutomationCondition::FreshnessViolated { max_lag_secs: 300 }, &c2),
            "unavailable suppresses firing even if a stale copy leaked through"
        );
        // Non-freshness conditions are unaffected by the flag.
        assert!(evaluate(&AutomationCondition::Eager, &c));
    }

    // ---- duration ----

    #[test]
    fn duration_forms() {
        assert_eq!(parse_duration("30s").unwrap(), 30);
        assert_eq!(parse_duration("5m").unwrap(), 300);
        assert_eq!(parse_duration("2h").unwrap(), 7200);
        assert_eq!(parse_duration("1d").unwrap(), 86400);
        assert_eq!(parse_duration("1h30m").unwrap(), 5400);
        assert!(parse_duration("5x").is_err());
        assert!(parse_duration("m").is_err());
        assert!(parse_duration("5").is_err());
    }

    // ---- cron ----

    #[test]
    fn cron_parse_and_next() {
        // Every day at 03:30.
        let c = CronExpr::parse("30 3 * * *").unwrap();
        let next = c.next_after(at("2026-10-05 10:00:00")).unwrap();
        assert_eq!(next, at("2026-10-06 03:30:00"));

        // Every 5 minutes.
        let c = CronExpr::parse("*/5 * * * *").unwrap();
        let next = c.next_after(at("2026-10-05 10:02:00")).unwrap();
        assert_eq!(next, at("2026-10-05 10:05:00"));

        // Hourly at :00 with a range + list.
        let c = CronExpr::parse("0 9-17 * * 1-5").unwrap();
        // Saturday 2026-10-03? No: 2026-10-05 is a Monday.
        let next = c.next_after(at("2026-10-05 18:00:00")).unwrap();
        assert_eq!(next, at("2026-10-06 09:00:00"));
        // Friday evening skips the weekend.
        let next = c.next_after(at("2026-10-09 18:00:00")).unwrap();
        assert_eq!(next, at("2026-10-12 09:00:00"));
    }

    #[test]
    fn cron_fires_between() {
        let c = CronExpr::parse("0 * * * *").unwrap(); // hourly
        // A fire (11:00) inside (10:05, 11:03].
        assert!(c.fires_between(at("2026-10-05 10:05:00"), at("2026-10-05 11:03:00")));
        // No fire inside (11:00, 11:59].
        assert!(!c.fires_between(at("2026-10-05 11:00:00"), at("2026-10-05 11:59:00")));
        // Boundary: fire exactly at now counts.
        assert!(c.fires_between(at("2026-10-05 10:30:00"), at("2026-10-05 11:00:00")));
    }

    #[test]
    fn cron_same_minute_double_tick_does_not_refire() {
        // Regression (codex review P0): two ticks inside the SAME matching
        // minute must not consume the fire time twice. Tick 1 at 12:00:05
        // fires and stamps last_ready=12:00:05; tick 2 at 12:00:35 must NOT
        // fire again — 12:00 <= last_eval.
        let c = CronExpr::parse("0 * * * *").unwrap();
        let tick1 = at("2026-10-05 12:00:05");
        assert!(c.fires_between(at("2026-10-05 11:30:00"), tick1));
        // The daemon stamps last_ready = tick time (12:00:05):
        let tick2 = at("2026-10-05 12:00:35");
        assert!(!c.fires_between(tick1, tick2), "same-minute re-tick must not refire");
        // ...and the next hour still fires normally.
        assert!(c.fires_between(tick1, at("2026-10-05 13:00:10")));
    }

    #[test]
    fn quoted_cron_and_duration_guards() {
        // Documented form with quotes must parse (codex review P1).
        assert!(parse_automation(r#"on_cron("*/5 * * * *")"#).is_ok());
        assert!(parse_automation(r#"on_interval("30s")"#).is_ok());
        // Mixed quoted/unquoted:
        assert!(parse_automation(r#"on_cron("0 * * * *") | freshness_violated(10m)"#).is_ok());
        // Unterminated quote errors:
        assert!(parse_automation(r#"on_cron("*/5 * * * *)"#).is_err());
        // 0s / oversized durations rejected (codex review P1):
        assert!(parse_automation("on_interval(0s)").is_err());
        assert!(parse_automation("on_interval(999999999999999999999d)").is_err());
    }

    // ---- parser precedence / errors ----

    #[test]
    fn parse_precedence_and_groups() {
        // a | b & c  ==  a | (b & c)
        let cond = parse_automation("eager | freshness_violated(5m) & on_interval(1h)").unwrap();
        assert_eq!(
            cond,
            AutomationCondition::Or(
                Box::new(AutomationCondition::Eager),
                Box::new(AutomationCondition::And(
                    Box::new(AutomationCondition::FreshnessViolated { max_lag_secs: 300 }),
                    Box::new(AutomationCondition::OnInterval { secs: 3600 }),
                )),
            )
        );
        // Parentheses flip it.
        let cond = parse_automation("(eager | freshness_violated(5m)) & on_interval(1h)").unwrap();
        assert!(matches!(cond, AutomationCondition::And(_, _)));
        // Not binds tightest.
        let cond = parse_automation("!eager & eager").unwrap();
        assert!(matches!(cond, AutomationCondition::And(_, _)));
        // Cron with spaces inside quotes-free parens.
        assert!(matches!(
            parse_automation("on_cron(*/5 * * * *)").unwrap(),
            AutomationCondition::OnCron(_)
        ));
        // Errors.
        assert!(parse_automation("bogus").is_err());
        assert!(parse_automation("on_cron(99 * * * *)").is_err());
        assert!(parse_automation("eager &").is_err());
        assert!(parse_automation("(eager").is_err());
    }

    // ---- THE truth table ----

    #[test]
    fn truth_table() {
        // now = 2026-10-05 12:00:00 (UTC, Monday) everywhere below.
        // Columns: condition × (mat, eval) -> expected.
        let cases: Vec<(&str, Option<&str>, Option<&str>, &str, bool)> = vec![
            // eager is always true.
            ("eager", None, None, "eager", true),
            ("eager", Some("2026-10-05 11:59:00"), None, "eager-fresh", true),
            // freshness_violated: never-run => violated.
            ("fv", None, None, "never-run", true),
            // materialized 10 min ago, max_lag 5m => violated.
            ("fv", Some("2026-10-05 11:50:00"), None, "10min>5m", true),
            // materialized 2 min ago, max_lag 5m => fine.
            ("fv", Some("2026-10-05 11:58:00"), None, "2min<5m", false),
            // exactly at the lag boundary counts as violated (>=).
            ("fv", Some("2026-10-05 11:55:00"), None, "exactly5m", true),
            // on_interval: last ready 10m ago, interval 5m => due.
            ("iv", Some("2026-10-05 11:00:00"), Some("2026-10-05 11:50:00"), "10m>5m", true),
            // last ready 2m ago, interval 5m => not due.
            ("iv", Some("2026-10-05 11:00:00"), Some("2026-10-05 11:58:00"), "2m<5m", false),
            // never evaluated => treat as long overdue => due.
            ("iv", None, None, "never-eval", true),
            // on_cron hourly: eval gate at 11:30 => 12:00 fire within => due.
            ("cron", None, Some("2026-10-05 11:30:00"), "fire-in-window", true),
            // eval gate at 12:00 (just ran) => next fire 13:00 > now => not due.
            ("cron", None, Some("2026-10-05 12:00:00"), "just-ran", false),
        ];
        for (kind, mat, eval, label, expected) in cases {
            let cond = match kind {
                "eager" => parse_automation("eager").unwrap(),
                "fv" => parse_automation("freshness_violated(5m)").unwrap(),
                "iv" => parse_automation("on_interval(5m)").unwrap(),
                "cron" => parse_automation("on_cron(0 * * * *)").unwrap(),
                _ => unreachable!(),
            };
            let c = ctx("2026-10-05 12:00:00", mat, eval);
            assert_eq!(evaluate(&cond, &c), expected, "case {label}");
        }
    }

    #[test]
    fn composite_conditions() {
        // The headline recipe: hourly cron OR freshness blown past 10m.
        let cond = parse_automation(r#"on_cron(0 * * * *) | freshness_violated(10m)"#).unwrap();
        // Off-cron, fresh -> idle.
        assert!(!evaluate(
            &cond,
            &ctx("2026-10-05 12:30:00", Some("2026-10-05 12:25:00"), Some("2026-10-05 12:00:00"))
        ));
        // Off-cron, but stale 15m -> fires via freshness.
        assert!(evaluate(
            &cond,
            &ctx("2026-10-05 12:30:00", Some("2026-10-05 12:10:00"), Some("2026-10-05 12:00:00"))
        ));

        // eager & !freshness_violated(1h): run often, but pause while very stale
        // (e.g. upstream broken — avoid hammering).
        let cond = parse_automation("eager & !freshness_violated(1h)").unwrap();
        assert!(evaluate(
            &cond,
            &ctx("2026-10-05 12:00:00", Some("2026-10-05 11:30:00"), None)
        ));
        assert!(!evaluate(
            &cond,
            &ctx("2026-10-05 12:00:00", Some("2026-10-05 10:00:00"), None)
        ));
        assert!(!evaluate(&cond, &ctx("2026-10-05 12:00:00", None, None)));
    }
}
