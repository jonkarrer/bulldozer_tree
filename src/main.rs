mod data;

use data::*;
use polars::prelude::*;

fn main() {
    let df = init_data_frame("data/train.csv").unwrap();
    // let mut df = fill_null_values(df).unwrap();
    // let mut df = date_part("saledate", df).unwrap();
    let mut df = nominal_encoding(&NOMINAL_COLUMNS, df).unwrap();

    // let nominal_columns = vec!["state", "saleyear", "salemonth", "saleday", "saledayofweek"];

    write_to_csv(&mut df).unwrap();

    // println!("{:?}", df.column("ProductSize").unwrap());
}
