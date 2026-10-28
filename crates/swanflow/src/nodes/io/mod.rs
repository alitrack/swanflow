//! I/O nodes — split from the original monolithic io.rs.
//! Subfiles: excel.rs (Excel nodes), nodes.rs (everything else).

pub mod excel;
pub mod nodes;
pub mod ossie;
pub mod ossie_export;
pub mod semantic_annotate;
pub mod semantic_extract;
pub mod semantic_fk;

// Re-export all public items from submodules.
// Note: ExcelScanLogical/ExcelScanMode/ExcelSinkLogical are defined
// in crate::core::datasource, not here — they are NOT re-exported
// to avoid E0252 (defined multiple times).
pub use excel::{
    ExcelReadNode, ExcelReportNode, ExcelSheetNamesNode, ExcelTemplateNode, ExcelWriteNode,
};
pub use nodes::{
    CsvReadNode, DbConnectorNode, DuckDBHelper, DuckDBReadNode, DuckDBWriteNode,
    GraphIndexWriterExec, GraphIndexWriterLogical, GraphIndexWriterNode, HttpFetchNode,
    ParquetReadNode, UniversalReadNode,
};
pub use ossie::OssieReadNode;
pub use ossie_export::OssieExportNode;
pub use semantic_annotate::SemanticAnnotateNode;
pub use semantic_extract::SemanticExtractNode;
pub use semantic_fk::SemanticFkInferNode;
