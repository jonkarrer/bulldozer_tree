use polars::prelude::*;
use std::fs::File;

const NOMINAL_COLUMNS: [&str; 35] = [
    "state",
    "ProductGroup",
    "Drive_System",
    "Enclosure",
    "Forks",
    "Pad_Type",
    "Ride_Control",
    "Stick",
    "Transmission",
    "Blade_Extension",
    "Engine_Horsepower",
    "Enclosure_Type",
    "Hydraulics",
    "Pushblock",
    "Ripper",
    "Scarifier",
    "Tip_Control",
    "Coupler",
    "Coupler_System",
    "Hydraulics_Flow",
    "Track_Type",
    "Thumb",
    "Pattern_Changer",
    "Grouser_Type",
    "Backhoe_Mounting",
    "Blade_Type",
    "Travel_Controls",
    "Differential_Type",
    "Steering_Controls",
    "Turbocharged",
    "Tire_Size",
    "Blade_Width",
    "Stick_Length",
    "Grouser_Tracks",
    "Undercarriage_Pad_Width",
];

const ORDINAL_COLUMNS: [&str; 2] = ["UsageBand", "ProductSize"];

const COLUMNS_TO_DROP: [&str; 12] = [
    "fiModelDesc",
    "fiBaseModel",
    "fiSecondaryDesc",
    "fiModelSeries",
    "fiModelDescriptor",
    "fiProductClassDesc",
    "ProductGroupDesc",
    "SalesID",
    "MachineID",
    "auctioneerID",
    "ModelID",
    "datasource",
];

pub fn clean_csv(path: &str) -> anyhow::Result<DataFrame> {
    let df = init_data_frame(path)?;
    let df = drop_columns(COLUMNS_TO_DROP.to_vec(), df);
    let df = nominal_encoding(&NOMINAL_COLUMNS, df)?;
    let df = nominal_encoding(&ORDINAL_COLUMNS, df)?;
    let df = fill_null_values(df)?;
    let df = date_part("saledate", df)?;

    Ok(df)
}

fn init_data_frame(path: &str) -> anyhow::Result<DataFrame> {
    Ok(CsvReadOptions::default()
        .with_has_header(true)
        .with_schema(None)
        .try_into_reader_with_file_path(Some(path.into()))?
        .finish()?)
}

fn nominal_encoding(columns: &[&str], df: DataFrame) -> Result<DataFrame, PolarsError> {
    let expressions: Vec<Expr> = columns
        .into_iter()
        .map(|col_name| {
            let dype = df.column(col_name).unwrap().dtype();
            let col_name = col_name.to_string();

            match dype {
                DataType::String => col(col_name)
                    .cast(DataType::Categorical(None, CategoricalOrdering::Physical))
                    .to_physical()
                    .cast(DataType::Float64),
                _ => col(col_name),
            }
        })
        .collect();

    df.lazy().with_columns(expressions).collect()
}

fn date_part(column_name: &str, df: DataFrame) -> Result<DataFrame, PolarsError> {
    let parsed_col_name = format!("{}_parsed", column_name);
    let year = format!("{}_year", column_name);
    let month = format!("{}_month", column_name);
    let weekday = format!("{}_weekday", column_name);
    let day = format!("{}_day", column_name);

    let expressions: Vec<Expr> = vec![
        col(&parsed_col_name)
            .dt()
            .year()
            .alias(year)
            .cast(DataType::Float64),
        col(&parsed_col_name)
            .dt()
            .month()
            .alias(month)
            .cast(DataType::Float64),
        col(&parsed_col_name)
            .dt()
            .weekday()
            .alias(weekday)
            .cast(DataType::Float64),
        col(&parsed_col_name)
            .dt()
            .day()
            .alias(day)
            .cast(DataType::Float64),
    ];

    let df = df
        .lazy()
        .with_column(
            col(column_name)
                .str()
                .strptime(
                    DataType::Date,
                    StrptimeOptions {
                        format: Some("%m/%d/%Y %H:%M".into()),
                        ..Default::default()
                    },
                    lit("raise"),
                )
                .alias(&parsed_col_name),
        )
        .collect()?;

    let df = df.lazy().with_columns(expressions).collect()?;
    let df = df.drop_many([column_name, &parsed_col_name]);

    Ok(df)
}

fn drop_columns(columns: Vec<&str>, df: DataFrame) -> DataFrame {
    df.drop_many(columns)
}

//  fn ordinal_encoding(columns: &[&str], df: DataFrame) -> Result<DataFrame, PolarsError> {
//     let expressions: Vec<Expr> = columns
//         .into_iter()
//         .map(|col_name| {
//             let dype = df.column(col_name).unwrap().dtype();
//             let col_name = col_name.to_string();
//             let mut utf8_array: Vec<String> = vec![String::from("Hello"), String::from("World")];

//             let rev_map = RevMapping::Local(utf8_array, 100);

//             match dype {
//                 DataType::String => col(col_name)
//                     .cast(DataType::Enum(Some(rev_map), CategoricalOrdering::Lexical))
//                     .to_physical(),
//                 _ => col(col_name),
//             }
//         })
//         .collect();

//     df.lazy().with_columns(expressions).collect()
// }

fn fill_null_values(df: DataFrame) -> Result<DataFrame, PolarsError> {
    let col_names = df.get_column_names_str();

    let expressions: Vec<Expr> = col_names
        .into_iter()
        .map(|col_name| {
            let dtype = df.column(col_name).unwrap().dtype().clone();

            match dtype {
                DataType::Int32 | DataType::Int64 | DataType::Int16 | DataType::Int8 => {
                    col(col_name).fill_null(col(col_name).drop_nulls().median())
                }
                DataType::Float32 | DataType::Float64 => {
                    col(col_name).fill_null(col(col_name).drop_nulls().median())
                }
                DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                    col(col_name).fill_null(col(col_name).drop_nulls().median())
                }
                DataType::String | DataType::Boolean | DataType::Categorical(_, _) => {
                    col(col_name).fill_null(col(col_name).drop_nulls().mode())
                }
                _ => col(col_name),
            }
        })
        .collect();

    df.lazy().with_columns(expressions).collect()
}

pub fn write_to_csv(df: &mut DataFrame, file_name: &str) -> anyhow::Result<()> {
    let mut file = File::create(file_name)?;
    Ok(CsvWriter::new(&mut file).include_header(true).finish(df)?)
}
