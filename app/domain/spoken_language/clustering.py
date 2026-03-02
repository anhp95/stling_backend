"""HDBSCAN clustering — pure computation."""

import io
import pandas as pd
from typing import Dict, Optional, Any


def cluster(
    csv_data: str,
    params: Optional[Dict] = None,
    **kwargs,
) -> Dict[str, Any]:
    """Cluster languages using HDBSCAN."""
    try:
        import hdbscan

        params = params or {}
        mcs = params.get("min_cluster_size", 5)
        ms = params.get("min_samples", 3)
        metric = params.get("metric", "jaccard")

        df = pd.read_csv(io.StringIO(csv_data))
        meta = [
            "Glottocode",
            "Language Family",
            "Language Name",
            "Latitude",
            "Longitude",
        ]
        meta = [c for c in meta if c in df.columns]
        concepts = [c for c in df.columns if c not in meta]
        if not concepts:
            return {
                "error": "No concept columns",
                "summary": {},
            }
        X = df[concepts].values
        clusterer = hdbscan.HDBSCAN(
            min_cluster_size=mcs,
            min_samples=ms,
            metric=metric,
            cluster_selection_method="eom",
        )
        labels = clusterer.fit_predict(X)
        df["cluster_id"] = labels
        n_clusters = int(len(set(labels)) - (1 if -1 in labels else 0))
        n_noise = int(sum(labels == -1))
        return {
            "csv_data": df.to_csv(index=False),
            "summary": {
                "total_clusters": n_clusters,
                "clustered_languages": int(len(labels) - n_noise),
                "noise_points": n_noise,
            },
        }
    except ImportError:
        return {
            "error": "hdbscan not installed",
            "summary": {},
        }
    except Exception as e:
        return {"error": str(e), "summary": {}}
