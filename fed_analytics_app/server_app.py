"""fed_analytics_app: A Flower / Federated Analytics app."""

import json
import random
import time
from collections.abc import Iterable
from logging import INFO

import numpy as np
from flwr.app import Context, Message, MessageType, RecordDict, ConfigRecord
from flwr.common.logger import log
from flwr.serverapp import Grid, ServerApp

app = ServerApp()


@app.main()
def main(grid: Grid, context: Context) -> None:
    """This `ServerApp` construct a histogram from partial-histograms reported by the
    `ClientApp`s."""

    min_nodes = 1
    fraction_sample = context.run_config["fraction-sample"]
    selected_features = str(context.run_config["selected-features"]).split(",")
    feature_aggregation = str(context.run_config["feature-aggregation"]).split(",")

    log(INFO, "")  # Add newline for log readability

    log(INFO, "=" * 60)
    log(INFO, "FEDERATED ANALYTICS CONFIGURATION".center(60))
    log(INFO, "=" * 60)
    log(INFO, "Selected features:")
    for i, feature in enumerate(selected_features, 1):
        log(INFO, "  %d. %s", i, feature.strip())
    log(INFO, "Feature aggregation methods: %s", ", ".join(feature_aggregation))
    log(INFO, "=" * 60)

    log(INFO, "")  # Add newline for log readability

    # Loop and wait until enough nodes are available.
    all_node_ids: list[int] = []
    while len(all_node_ids) < min_nodes:
        all_node_ids = list(grid.get_node_ids())
        if len(all_node_ids) >= min_nodes:
            # Sample nodes
            num_to_sample = int(len(all_node_ids) * fraction_sample)
            node_ids = random.sample(all_node_ids, num_to_sample)
            break
        log(INFO, "Waiting for nodes to connect...")
        time.sleep(2)

    log(INFO, "Sampled %s nodes (out of %s)", len(node_ids), len(all_node_ids))

    # Create messages
    config = ConfigRecord(
        {
            "selected_features": selected_features,
            "feature_aggregation": feature_aggregation,
        }
    )
    recorddict = RecordDict({"config": config})
    messages = []
    for node_id in node_ids:  # one message for each node
        message = Message(
            content=recorddict,
            message_type=MessageType.QUERY,  # target `query` method in ClientApp
            dst_node_id=node_id,
            group_id="1",
        )
        messages.append(message)

    # Send messages and wait for all results
    replies = grid.send_and_receive(messages)
    log(INFO, "Received %s/%s results", len(list(replies)), len(messages))

    aggregated_stats = aggregate_features(
        replies, selected_features, feature_aggregation
    )

    # Display final aggregated stats
    print("\n" + "=" * 40)
    print("FINAL AGGREGATED STATISTICS".center(40))
    print("=" * 40)
    print(json.dumps(aggregated_stats, indent=2))
    print("=" * 40 + "\n")


def aggregate_features(
    messages: Iterable[Message],
    selected_features: list[str],
    feature_aggregation: list[str],
) -> dict[str, dict[str, float | None]]:
    """Aggregate feature statistics from client messages.

    Args:
        messages: Messages from client nodes containing query results
        selected_features: List of feature names to aggregate
        feature_aggregation: List of aggregation methods ('mean', 'std')

    Returns:
        Dictionary with aggregated statistics for each feature and method
    """

    def _initialize_aggregation_stats() -> dict[str, dict[str, dict[str, float]]]:
        """Initialize nested dictionary structure for aggregation statistics."""
        stats: dict[str, dict[str, dict[str, float]]] = {}
        for feature in selected_features:
            stats[feature] = {}
            for agg_method in feature_aggregation:
                if agg_method == "mean":
                    stats[feature]["mean"] = {"sum": 0.0, "count": 0}
                elif agg_method == "std":
                    stats[feature]["std"] = {"sum": 0.0, "count": 0, "sum_sqd": 0.0}
        return stats

    def _accumulate_statistics(
        aggregated_stats: dict, query_results, feature: str, agg_method: str
    ) -> None:
        """Accumulate statistics from a single client's query results."""
        # Handle different RecordDict types from flwr
        if hasattr(query_results, "__getitem__"):
            results = query_results
        else:
            results = dict(query_results)

        if agg_method == "mean":
            aggregated_stats[feature]["mean"]["sum"] += results[f"{feature}_mean_sum"]
            aggregated_stats[feature]["mean"]["count"] += results[
                f"{feature}_mean_count"
            ]
        elif agg_method == "std":
            aggregated_stats[feature]["std"]["sum"] += results[f"{feature}_std_sum"]
            aggregated_stats[feature]["std"]["count"] += results[f"{feature}_std_count"]
            aggregated_stats[feature]["std"]["sum_sqd"] += results[
                f"{feature}_std_sum_sqd"
            ]

    def _compute_final_statistics(
        aggregated_stats: dict, feature: str, agg_method: str
    ) -> float:
        """Compute final aggregated statistic for a feature and aggregation method."""
        if agg_method == "mean":
            stats = aggregated_stats[feature]["mean"]
            if stats["count"] == 0:
                raise ValueError(
                    f"No data available for feature '{feature}' mean calculation"
                )
            return stats["sum"] / stats["count"]

        elif agg_method == "std":
            stats = aggregated_stats[feature]["std"]
            if stats["count"] <= 1:
                raise ValueError(
                    f"Insufficient data for feature '{feature}' std calculation (need > 1 samples)"
                )

            mean = stats["sum"] / stats["count"]
            # Using the formula: Var = (sum_sqd - n * mean^2) / (n - 1)
            variance = (stats["sum_sqd"] - stats["count"] * mean**2) / (
                stats["count"] - 1
            )

            if variance < 0:
                # Handle numerical precision issues
                variance = 0.0

            return np.sqrt(variance)

        else:
            raise ValueError(f"Unsupported aggregation method: {agg_method}")

    # Initialize aggregated statistics
    aggregated_stats = _initialize_aggregation_stats()

    # Aggregate statistics from all valid client messages
    valid_message_count = 0
    for message in messages:
        if message.has_error():
            continue

        query_results = message.content["query_results"]
        valid_message_count += 1

        for feature in selected_features:
            for agg_method in feature_aggregation:
                _accumulate_statistics(
                    aggregated_stats, query_results, feature, agg_method
                )

    if valid_message_count == 0:
        raise ValueError("No valid messages received from clients")

    # Compute final aggregated statistics
    final_stats: dict[str, dict[str, float | None]] = {}
    for feature in selected_features:
        final_stats[feature] = {}
        for agg_method in feature_aggregation:
            try:
                final_stats[feature][agg_method] = _compute_final_statistics(
                    aggregated_stats, feature, agg_method
                )
            except ValueError as e:
                log(INFO, "Warning: %s", e)
                final_stats[feature][agg_method] = None

    return final_stats
