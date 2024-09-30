mod processing;
use linfa::{Dataset, DatasetBase};
use ndarray::{Array, ArrayBase, Dim, OwnedRepr};
pub use processing::*;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct BulldozerModel {
    year_made: f64,
    machine_hours_current_meter: f64,
    usage_band: f64,
    product_size: f64,
    state: Option<f64>,
    product_group: f64,
    drive_system: f64,
    enclosure: f64,
    forks: f64,
    pad_type: f64,
    ride_control: f64,
    stick: f64,
    transmission: f64,
    turbocharged: f64,
    blade_extension: f64,
    blade_width: f64,
    enclosure_type: f64,
    engine_horsepower: f64,
    hydraulics: f64,
    pushblock: f64,
    ripper: f64,
    scarifier: f64,
    tip_control: f64,
    tire_size: f64,
    coupler: f64,
    coupler_system: f64,
    grouser_tracks: f64,
    hydraulics_flow: f64,
    track_type: f64,
    undercarriage_pad_width: f64,
    stick_length: f64,
    thumb: f64,
    pattern_changer: f64,
    grouser_type: f64,
    backhoe_mounting: f64,
    blade_type: f64,
    travel_controls: f64,
    differential_type: f64,
    steering_controls: f64,
    saledate_year: f64,
    saledate_month: f64,
    saledate_weekday: f64,
    saledate_day: f64,
}

#[derive(Debug)]
pub struct BulldozerDataset(Vec<BulldozerModel>);
impl BulldozerDataset {
    pub fn to_vec(path: &str) -> (Vec<Vec<f64>>, Vec<f64>) {
        let mut reader = csv::ReaderBuilder::new()
            .from_path(std::path::Path::new(path))
            .expect("could not read csv");

        let mut features: Vec<Vec<f64>> = Vec::new();
        let mut targets: Vec<f64> = Vec::new();

        for result in reader.records() {
            let mut row: Vec<f64> = result
                .unwrap()
                .iter()
                .map(|field| field.parse::<f64>().unwrap_or(0.0))
                .collect();
            let sale_price = row.remove(0);

            targets.push(sale_price);
            features.push(row);
        }

        (features, targets)
    }

    pub fn get_training_set() -> DatasetBase<
        ArrayBase<OwnedRepr<f64>, Dim<[usize; 2]>>,
        ArrayBase<OwnedRepr<usize>, Dim<[usize; 1]>>,
    > {
        let (features, targets) = Self::to_vec("data/processed/train.csv");

        let feat_array = Array::from_shape_vec(
            (features.len(), features[0].len()),
            features.into_iter().flatten().collect(),
        )
        .unwrap();

        let target_array = Array::from_vec(targets.into_iter().map(|x| x as usize).collect());
        Dataset::new(feat_array, target_array)
    }

    pub fn get_validation_set() -> DatasetBase<
        ArrayBase<OwnedRepr<f64>, Dim<[usize; 2]>>,
        ArrayBase<OwnedRepr<usize>, Dim<[usize; 1]>>,
    > {
        let (features, targets) = Self::to_vec("data/processed/valid.csv");

        let feat_array = Array::from_shape_vec(
            (features.len(), features[0].len()),
            features.into_iter().flatten().collect(),
        )
        .unwrap();

        let target_array = Array::from_vec(targets.into_iter().map(|x| x as usize).collect());
        Dataset::new(feat_array, target_array)
    }
}
