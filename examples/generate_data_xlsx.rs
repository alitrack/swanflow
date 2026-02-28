use rust_xlsxwriter::Workbook;

fn main() {
    let mut workbook = Workbook::new();
    let worksheet = workbook.add_worksheet();
    worksheet.write(0, 0, "Name").unwrap();
    worksheet.write(0, 1, "Score").unwrap();
    worksheet.write(1, 0, "Alice").unwrap();
    worksheet.write(1, 1, 95.5).unwrap();
    worksheet.write(2, 0, "Bob").unwrap();
    worksheet.write(2, 1, 88.0).unwrap();
    worksheet.write(3, 0, "Charlie").unwrap();
    worksheet.write(3, 1, 92.0).unwrap();
    workbook.save("data.xlsx").unwrap();
    println!("Generated data.xlsx");
}
