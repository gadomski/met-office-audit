"""Tests for the audit comparison logic."""

from met_office_audit.audit import AuditResult, compare_item_assets


def test_no_missing_items_or_assets():
    s3 = {"item-a": {"asset-1", "asset-2"}, "item-b": {"asset-3"}}
    pc = {"item-a": {"asset-1", "asset-2"}, "item-b": {"asset-3"}}
    result = compare_item_assets(s3, pc)
    assert result == AuditResult(
        missing_assets=[],
        completely_missing_items=[],
        complete_item_count=2,
        incomplete_item_count=0,
    )


def test_completely_missing_item():
    s3 = {"item-a": {"asset-1"}, "item-b": {"asset-2"}}
    pc = {"item-a": {"asset-1"}}
    result = compare_item_assets(s3, pc)
    assert "item-b" in result.completely_missing_items
    assert ("item-b", "asset-2") in result.missing_assets
    assert result.complete_item_count == 1
    assert result.incomplete_item_count == 0


def test_partially_missing_assets():
    s3 = {"item-a": {"asset-1", "asset-2", "asset-3"}}
    pc = {"item-a": {"asset-1"}}
    result = compare_item_assets(s3, pc)
    assert result.completely_missing_items == []
    assert result.incomplete_item_count == 1
    assert result.complete_item_count == 0
    assert ("item-a", "asset-2") in result.missing_assets
    assert ("item-a", "asset-3") in result.missing_assets
    assert ("item-a", "asset-1") not in result.missing_assets


def test_extra_pc_assets_are_ignored():
    s3 = {"item-a": {"asset-1"}}
    pc = {"item-a": {"asset-1", "asset-extra"}}
    result = compare_item_assets(s3, pc)
    assert result == AuditResult(
        missing_assets=[],
        completely_missing_items=[],
        complete_item_count=1,
        incomplete_item_count=0,
    )


def test_empty_s3():
    result = compare_item_assets({}, {"item-a": {"asset-1"}})
    assert result == AuditResult(
        missing_assets=[],
        completely_missing_items=[],
        complete_item_count=0,
        incomplete_item_count=0,
    )


def test_all_items_completely_missing():
    s3 = {"item-a": {"asset-1"}, "item-b": {"asset-2"}}
    result = compare_item_assets(s3, {})
    assert set(result.completely_missing_items) == {"item-a", "item-b"}
    assert result.complete_item_count == 0
    assert result.incomplete_item_count == 0
    assert len(result.missing_assets) == 2


def test_mixed_complete_incomplete_and_missing():
    s3 = {
        "item-complete": {"asset-1", "asset-2"},
        "item-incomplete": {"asset-1", "asset-2"},
        "item-missing": {"asset-1"},
    }
    pc = {
        "item-complete": {"asset-1", "asset-2"},
        "item-incomplete": {"asset-1"},
    }
    result = compare_item_assets(s3, pc)
    assert result.complete_item_count == 1
    assert result.incomplete_item_count == 1
    assert result.completely_missing_items == ["item-missing"]
    assert ("item-incomplete", "asset-2") in result.missing_assets
    assert ("item-missing", "asset-1") in result.missing_assets
    assert len(result.missing_assets) == 2
