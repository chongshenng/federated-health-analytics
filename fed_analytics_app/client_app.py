"""fed_analytics_app: A Flower / Federated Analytics app."""

import warnings

import numpy as np
from flwr.app import Context, Message, MetricRecord, RecordDict
from flwr.clientapp import ClientApp
import pandas as pd

warnings.filterwarnings("ignore", category=UserWarning)

# Flower ClientApp
app = ClientApp()


@app.query()
def query(msg: Message, context: Context):
    """Construct histogram of local dataset and report to `ServerApp`."""

    dataset_path = context.node_config["dataset-path"]

    df = pd.read_csv(dataset_path)

    selected_features = msg.content["config"]["selected_features"]
    feature_aggregation = msg.content["config"]["feature_aggregation"]

    metrics = {}
    for feature in selected_features:
        if feature not in df.columns:
            raise ValueError(f"Feature '{feature}' not found in dataset columns.")

        for agg in feature_aggregation:
            if agg == "mean":
                metrics[f"{feature}_{agg}_sum"] = sum(df[feature])
                metrics[f"{feature}_{agg}_count"] = len(df[feature])
            elif agg == "std":
                metrics[f"{feature}_{agg}_sum"] = sum(df[feature])
                metrics[f"{feature}_{agg}_count"] = len(df[feature])
                metrics[f"{feature}_{agg}_sum_sqd"] = sum(df[feature] ** 2)
            else:
                print(f"Aggregation method '{agg}' not recognized.")

    reply_content = RecordDict({"query_results": MetricRecord(metrics)})

    return Message(reply_content, reply_to=msg)
