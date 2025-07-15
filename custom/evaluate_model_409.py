import numpy as np
from sklearn.metrics import recall_score
from sklearn.metrics import confusion_matrix
from sklearn.metrics import accuracy_score
from typing import Any
from typing import Dict
from sklearn.metrics import precision_score
from sklearn.metrics import f1_score


@custom
def evaluate_model(model: Any, *args, **kwargs):
    """
    Evaluate the performance of a trained model using various metrics.

    Args:
        model (Any): The trained model object with a predict method.
        validation_data (Dict[str, np.ndarray]): A dictionary containing validation features and labels.
            Expected keys: 'features' and 'labels'.

    Returns:
        Dict[str, Any]: A dictionary containing evaluation metrics.
    """

    # Extract features and labels from validation data
    validation_data: Dict[str, np.ndarray] = None
    features: np.ndarray = validation_data.get("features")
    labels: np.ndarray = validation_data.get("labels")

    # Check if features and labels are provided
    if features is None or labels is None:
        raise ValueError("Validation data must contain 'features' and 'labels' keys.")

    # Generate predictions using the trained model
    predictions: np.ndarray = model.predict(features)

    # Calculate evaluation metrics
    accuracy: float = accuracy_score(labels, predictions)
    precision: float = precision_score(
        labels, predictions, average="weighted", zero_division=0
    )
    recall: float = recall_score(
        labels, predictions, average="weighted", zero_division=0
    )
    f1: float = f1_score(labels, predictions, average="weighted", zero_division=0)
    conf_matrix: np.ndarray = confusion_matrix(labels, predictions)

    # Prepare the report dictionary
    report: Dict[str, Any] = {
        "accuracy": accuracy,
        "precision": precision,
        "recall": recall,
        "f1_score": f1,
        "confusion_matrix": conf_matrix.tolist(),  # Convert to list for JSON serializability
    }

    return report
