use duckdb::Connection;

fn main() -> anyhow::Result<()> {
    let conn = Connection::open("target/swanflow_cache/metadata.duckdb")?;
    
    println!("--- NODES ---");
    let mut stmt = conn.prepare("SELECT * FROM __graph_nodes")?;
    let rows = stmt.query_map([], |row| {
        let id: i64 = row.get(0)?;
        let label: String = row.get(1)?;
        let props: String = row.get(2)?;
        Ok((id, label, props))
    })?;
    for row in rows {
        let (id, label, props) = row?;
        println!("ID: {}, Label: {}, Props: {}", id, label, props);
    }

    println!("
--- EDGES ---");
    let mut stmt = conn.prepare("SELECT * FROM __graph_edges")?;
    let rows = stmt.query_map([], |row| {
        let src: i64 = row.get(0)?;
        let dst: i64 = row.get(1)?;
        let edge_type: String = row.get(2)?;
        let weight: f64 = row.get(3)?;
        Ok((src, dst, edge_type, weight))
    })?;
    for row in rows {
        let (src, dst, edge_type, weight) = row?;
        println!("Src: {}, Dst: {}, Type: {}, Weight: {}", src, dst, edge_type, weight);
    }

    Ok(())
}
