use std::{fs::File, io::Write};

use data::{write_to_csv, BulldozerDataset};
use ndarray::{Array, Array1, Array2, ArrayBase};

use linfa::prelude::*;
use linfa_trees::{DecisionTree, Result, SplitQuality};

mod data;
mod tree;

fn main() {
    let mut training_df = data::clean_csv("data/train.csv").unwrap();
    write_to_csv(&mut training_df, "data/processed/train.csv").unwrap();
    let mut validation_df = data::clean_csv("data/valid.csv").unwrap();
    write_to_csv(&mut validation_df, "data/processed/valid.csv").unwrap();

    let training_set = data::BulldozerDataset::get_training_set();
    let validation_set = data::BulldozerDataset::get_validation_set();

    println!("Training model with Gini criterion ...");
    let gini_model = DecisionTree::params()
        .split_quality(SplitQuality::Gini)
        .max_depth(Some(5))
        .min_weight_split(1.0)
        .min_weight_leaf(1.0)
        .fit(&training_set)
        .unwrap();

    let gini_pred_y = gini_model.predict(&validation_set);
    let cm = gini_pred_y.confusion_matrix(&validation_set).unwrap();

    println!("{:?}", cm);

    println!(
        "Test accuracy with Gini criterion: {:.2}%",
        100.0 * cm.accuracy()
    );

    let feats = gini_model.features();
    println!("Features trained in this tree {:?}", feats);

    println!("Training model with entropy criterion ...");
    let entropy_model = DecisionTree::params()
        .split_quality(SplitQuality::Entropy)
        .max_depth(Some(5))
        .min_weight_split(10.0)
        .min_weight_leaf(10.0)
        .fit(&training_set)
        .unwrap();

    let entropy_pred_y = entropy_model.predict(&validation_set);
    let cm = entropy_pred_y.confusion_matrix(&validation_set).unwrap();

    println!("{:?}", cm);

    println!(
        "Test accuracy with Entropy criterion: {:.2}%",
        100.0 * cm.accuracy()
    );

    let feats = entropy_model.features();
    println!("Features trained in this tree {:?}", feats);

    let mut tikz = File::create("decision_tree_example.tex").unwrap();
    tikz.write_all(
        gini_model
            .export_to_tikz()
            .with_legend()
            .to_string()
            .as_bytes(),
    )
    .unwrap();
    println!(" => generate Gini tree description with `latex decision_tree_example.tex`!");
}
