from __future__ import annotations

import hashlib
import json

import httpx
import pytest
import respx

from activities.download_se_hvd_dataset import download_se_hvd_dataset
from contracts import DownloadSourceFilesInput


def _sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


@respx.mock
@pytest.mark.asyncio
async def test_se_hvd_downloads_configured_datasets_with_stable_snapshot_id(tmp_path, monkeypatch):
    datasets = [
        {
            "dataset": "organisationer",
            "url": "https://bolagsverket.example.test/hvd/organisationer.json",
            "format": "json",
        },
        {
            "dataset": "arsredovisningar",
            "url": "https://bolagsverket.example.test/hvd/arsredovisningar.json",
            "format": "json",
        },
    ]
    monkeypatch.setenv("SE_HVD_DATASETS_JSON", json.dumps(datasets))
    respx.get(datasets[0]["url"]).mock(return_value=httpx.Response(200, content=b'{"data":[]}'))
    respx.get(datasets[1]["url"]).mock(return_value=httpx.Response(200, content=b'{"documents":[]}'))

    result = await download_se_hvd_dataset(
        DownloadSourceFilesInput(source="se", mode="full", output_dir=str(tmp_path), snapshot_id="se-snapshot")
    )

    assert result.source == "se"
    assert result.snapshot_id == "se-snapshot"
    assert [file.dataset for file in result.files] == ["organisationer", "arsredovisningar"]
    assert [file.file_path for file in result.files] == [
        str(tmp_path / "se-organisationer-se-snapshot.json"),
        str(tmp_path / "se-arsredovisningar-se-snapshot.json"),
    ]
    assert result.files[0].sha256 == _sha256(b'{"data":[]}')
    assert result.files[1].sha256 == _sha256(b'{"documents":[]}')


@pytest.mark.asyncio
async def test_se_hvd_rejects_missing_dataset_config(tmp_path, monkeypatch):
    monkeypatch.delenv("SE_HVD_DATASETS_JSON", raising=False)

    with pytest.raises(RuntimeError, match="SE_HVD_DATASETS_JSON"):
        await download_se_hvd_dataset(
            DownloadSourceFilesInput(source="se", mode="full", output_dir=str(tmp_path), snapshot_id="safe")
        )
