from unittest.mock import MagicMock, patch
from transformer.transform import TransformationPipeline


@patch("transformer.transform.Minio")
@patch("transformer.transform.MongoClient")
def test_transformation_pipeline_init(mock_mongo, mock_minio):
    pipeline = TransformationPipeline()
    assert pipeline is not None
    mock_mongo.assert_called_once()
    mock_minio.assert_called_once()


@patch("transformer.transform.Minio")
@patch("transformer.transform.MongoClient")
def test_transformation_pipeline_run_no_docs(mock_mongo, mock_minio):
    pipeline = TransformationPipeline()

    # Mock find to return empty
    mock_meta = MagicMock()
    mock_meta.find.return_value = []
    pipeline.landing_meta = mock_meta

    result = pipeline.run("2000-01-01", "2000-01-31")
    assert result["total"] == 0
    assert result["transformed"] == 0


@patch("transformer.transform.Minio")
@patch("transformer.transform.MongoClient")
def test_transformation_pipeline_run_with_docs(mock_mongo, mock_minio):
    pipeline = TransformationPipeline()

    doc = {
        "_id": "someid",
        "identifier": "ADJ-1",
        "date": "2000-01-15",
        "body": "wrc",
        "file_path": "path/1.html",
    }

    mock_meta = MagicMock()
    mock_meta.find.return_value = [doc]
    pipeline.landing_meta = mock_meta

    # Mock MinIO get_object
    mock_response = MagicMock()
    mock_response.read.return_value = b"<html><body>Test</body></html>"
    pipeline.minio_client.get_object.return_value = mock_response

    result = pipeline.run("2000-01-01", "2000-01-31")
    assert result["total"] == 1
    assert result["transformed"] == 1
