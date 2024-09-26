mod data;

use data::*;
use polars::prelude::*;

fn main() {
    let df = init_data_frame("data/train.csv").unwrap();
    let df = drop_descriptions(COLUMNS_TO_DROP.to_vec(), df);
    let df = fill_null_values(df).unwrap();
    let df = nominal_encoding(&NOMINAL_COLUMNS, df).unwrap();
    let df = nominal_encoding(&ORDINAL_COLUMNS, df).unwrap();
    let mut df = date_part("saledate", df).unwrap();
    write_to_csv(&mut df).unwrap();

    // println!("{:?}", df.column("ProductSize").unwrap());
}
