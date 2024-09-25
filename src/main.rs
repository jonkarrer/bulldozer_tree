mod data;

use data::*;
use polars::prelude::*;

fn main() {
    let df = init_data_frame("data/train.csv").unwrap();
    // let df = fill_null_with_mode(&df, "ProductSize").unwrap();
    // write_to_csv(&mut df).unwrap();

    let col_names = df.get_column_names_str();

    let expressions: Vec<Expr> = col_names
        .into_iter()
        .map(|col_name| {
            let dtype = df.column(col_name).unwrap().dtype().clone();

            match dtype {
                DataType::Int32 | DataType::Int64 | DataType::Int16 | DataType::Int8 => {
                    col(col_name).fill_null(col(col_name).drop_nulls().mean())
                }
                DataType::Float32 | DataType::Float64 => {
                    col(col_name).fill_null(col(col_name).drop_nulls().mean())
                }
                DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                    col(col_name).fill_null(col(col_name).drop_nulls().mean())
                }
                DataType::String | DataType::Boolean | DataType::Categorical(_, _) => {
                    col(col_name).fill_null(col(col_name).drop_nulls().mode())
                }
                _ => col(col_name),
            }
        })
        .collect();

    let mut df = df.lazy().with_columns(expressions).collect().unwrap();
    write_to_csv(&mut df).unwrap();
}
