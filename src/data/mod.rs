use polars::prelude::*;
use std::fs::File;

struct CsvRecord {
    pub sales_id: Option<String>,
    pub machine_id: Option<String>,
    pub model_id: Option<String>,
    pub datasource: Option<String>,    // nix
    pub auctioneer_id: Option<String>, // nix
    pub year_made: Option<String>,     // * convert to numerical
    pub machine_hours_current_meter: Option<String>,
    pub usage_band: Option<String>,            // * convert to ordinal
    pub saledate: Option<String>,              // * expand
    pub fi_model_desc: Option<String>,         // nix
    pub fi_base_model: Option<String>,         // * convert to nominal
    pub fi_secondary_desc: Option<String>,     // nix
    pub fi_model_series: Option<String>,       // * convert to nominal
    pub fi_model_descriptor: Option<String>,   // nix
    pub product_size: Option<String>,          // * convert to ordinal
    pub fi_product_class_desc: Option<String>, // nix
    pub state: Option<String>,                 // * convert to nominal
    pub product_group: Option<String>,         // * convert to nominal
    pub product_group_desc: Option<String>,    // nix
    pub drive_system: Option<String>,          // * convert to nominal
    pub enclosure: Option<String>,             // * convert to nominal
    pub forks: Option<String>,                 // * convert to nominal
    pub pad_type: Option<String>,              // * convert to nominal
    pub ride_control: Option<String>,          // * convert to nominal
    pub stick: Option<String>,
    pub transmission: Option<String>,
    pub turbocharged: Option<String>,
    pub blade_extension: Option<String>,
    pub blade_width: Option<String>,
    pub enclosure_type: Option<String>,
    pub engine_horsepower: Option<String>,
    pub hydraulics: Option<String>,
    pub pushblock: Option<String>,
    pub ripper: Option<String>,
    pub scarifier: Option<String>,
    pub tip_control: Option<String>,
    pub tire_size: Option<String>,
    pub coupler: Option<String>,
    pub coupler_system: Option<String>,
    pub grouser_tracks: Option<String>,
    pub hydraulics_flow: Option<String>,
    pub track_type: Option<String>,
    pub undercarriage_pad_width: Option<String>,
    pub stick_length: Option<String>,
    pub thumb: Option<String>,
    pub pattern_changer: Option<String>,
    pub grouser_type: Option<String>,
    pub backhoe_mounting: Option<String>,
    pub blade_type: Option<String>,
    pub travel_controls: Option<String>,
    pub differential_type: Option<String>,
    pub steering_controls: Option<String>,
}

struct BulldozerModel {
    pub sales_id: String,
    pub machine_id: String,
    pub model_id: String,
    pub year_made: f32,
    pub machine_hours_current_meter: Option<String>,
    pub usage_band: Option<String>,
    pub saledate: Option<String>, // ! need to expand column
    pub fi_base_model: Option<String>,
    pub fi_model_series: Option<String>,
    pub product_size: Option<String>,
    pub state: Option<String>,
    pub product_group: Option<String>,
    pub drive_system: Option<String>,
    pub enclosure: Option<String>,
    pub forks: Option<String>,
    pub pad_type: Option<String>,
    pub ride_control: Option<String>,
    pub stick: Option<String>,
    pub transmission: Option<String>,
    pub turbocharged: Option<String>,
    pub blade_extension: Option<String>,
    pub blade_width: Option<String>,
    pub enclosure_type: Option<String>,
    pub engine_horsepower: Option<String>,
    pub hydraulics: Option<String>,
    pub pushblock: Option<String>,
    pub ripper: Option<String>,
    pub scarifier: Option<String>,
    pub tip_control: Option<String>,
    pub tire_size: Option<String>,
    pub coupler: Option<String>,
    pub coupler_system: Option<String>,
    pub grouser_tracks: Option<String>,
    pub hydraulics_flow: Option<String>,
    pub track_type: Option<String>,
    pub undercarriage_pad_width: Option<String>,
    pub stick_length: Option<String>,
    pub thumb: Option<String>,
    pub pattern_changer: Option<String>,
    pub grouser_type: Option<String>,
    pub backhoe_mounting: Option<String>,
    pub blade_type: Option<String>,
    pub travel_controls: Option<String>,
    pub differential_type: Option<String>,
    pub steering_controls: Option<String>,
    pub was_filled_in: bool, // ! needed for filling missing data
}

pub fn init_data_frame(path: &str) -> anyhow::Result<DataFrame> {
    Ok(CsvReadOptions::default()
        .with_has_header(true)
        .with_schema(None)
        .try_into_reader_with_file_path(Some(path.into()))?
        .finish()?)
}

pub fn nominal_encoding(column_name: &str, df: &DataFrame) -> Result<DataFrame, PolarsError> {
    df.clone()
        .lazy()
        .with_column(
            col(column_name).cast(DataType::Categorical(None, CategoricalOrdering::Lexical)),
        )
        .collect()
}

pub fn date_part(column_name: &str, df: DataFrame) -> Result<DataFrame, PolarsError> {
    let parsed_col_name = format!("{}_parsed", column_name);
    let year = format!("{}_year", column_name);
    let month = format!("{}_month", column_name);
    let weekday = format!("{}_weekday", column_name);
    let day = format!("{}_day", column_name);

    let expressions: Vec<Expr> = vec![
        col(&parsed_col_name).dt().year().alias(year),
        col(&parsed_col_name).dt().month().alias(month),
        col(&parsed_col_name).dt().weekday().alias(weekday),
        col(&parsed_col_name).dt().day().alias(day),
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

    Ok(df.lazy().with_columns(expressions).collect()?)
}

pub fn fill_null_values(df: DataFrame) -> Result<DataFrame, PolarsError> {
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

    Ok(df.lazy().with_columns(expressions).collect()?)
}

pub fn write_to_csv(df: &mut DataFrame) -> anyhow::Result<()> {
    let mut file = File::create("processed_train.csv")?;
    Ok(CsvWriter::new(&mut file).include_header(true).finish(df)?)
}
