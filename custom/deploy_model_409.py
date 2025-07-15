import requests
import os
import joblib


@custom
def main(trained_model: object, *args, **kwargs):
    """
    Deploy the trained model to a production environment or model registry.

    Args:
        trained_model (object): The trained model object to be deployed.
        **kwargs: Additional keyword arguments for deployment configuration.

    Returns:
        str: Deployment confirmation message or API endpoint URL.
    """

    # Example: Export the model to a file (e.g., pickle, joblib, or custom format)
    model_export_path = "/tmp/deployed_model.pkl"
    try:
        # Save the model to a file
        joblib.dump(trained_model, model_export_path)
    except ImportError:
        raise ImportError("Please install joblib to export the model.")

    # Example: Upload the model to a model registry or deployment platform
    # This example assumes an API endpoint for deployment
    deployment_api_url = os.environ.get("MODEL_DEPLOYMENT_API_URL")
    api_key = os.environ.get("MODEL_DEPLOYMENT_API_KEY")

    if not deployment_api_url or not api_key:
        raise ValueError(
            "Deployment API URL or API key not set in environment variables."
        )

    with open(model_export_path, "rb") as model_file:
        files = {"file": ("model.pkl", model_file, "application/octet-stream")}
        headers = {"Authorization": f"Bearer {api_key}"}
        response = requests.post(deployment_api_url, files=files, headers=headers)

    if response.status_code != 200:
        raise RuntimeError(
            f"Model deployment failed with status code {response.status_code}: {response.text}"
        )

    # Parse response to get deployment endpoint or confirmation
    response_data = response.json()
    deployment_endpoint = response_data.get("endpoint_url", "")

    if not deployment_endpoint:
        return "Model deployed successfully, but no endpoint URL returned."

    return f"Model successfully deployed at {deployment_endpoint}"
