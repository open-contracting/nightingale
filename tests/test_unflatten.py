from pathlib import Path
from unittest import mock

import dict_hash
import pytest

from nightingale.config import Config, Datasource, Mapping, Output, Publishing
from nightingale.mapper import OCDSDataMapper
from nightingale.utils import get_longest_array_path


class dummy_ocds_mapping_config:
    def __init__(self, mapping_items, array_paths=None):
        self.mappings = mapping_items
        self.array_paths = array_paths or []

    def get_paths_for_mapping(self, flat_column, force_publish=False):
        for item in self.mappings:
            if item.get("mapping") == flat_column:
                return [item["path"]]
        return []

    def get_ocid_mapping(self):
        return "ocid"

    def get_containing_array_path(self, path):
        return get_longest_array_path(self.array_paths, path)


class dummy_ocds_mapping_template:
    def __init__(self, dummy_config: dummy_ocds_mapping_config, schema: dict):
        self.mappings = dummy_config.mappings
        self._schema = schema
        self.array_paths = dummy_config.array_paths

    def get_schema(self):
        return self._schema

    def get_ocid_mapping(self):
        return "ocid"

    def get_containing_array_path(self, path):
        return get_longest_array_path(self.array_paths, path)

    def get_datetime_fields(self):
        return set()


@pytest.fixture
def base_config():
    datasource = Datasource(connection="sqlite:///:memory:")
    mapping = Mapping(name="OCDS_Step1", file=Path("/dummy/path.xlsx"), selector="OCDS_Selector", ocid_prefix="prefix")
    publishing = Publishing(version="1.0", publisher="OCDS Publisher", base_uri="http://example.com")
    output = Output(directory=Path("/dummy/output"))
    return Config(datasource=datasource, mapping=mapping, publishing=publishing, output=output)


@pytest.mark.parametrize(
    "rows, mapping_items, schema_def, array_paths, expected_output",
    [
        (
            [
                {
                    "BuyerID": "B123",
                    "BuyerName": "Dept of Education",
                    "TenderID": "T456",
                    "TenderTitle": "Supply of textbooks",
                    "TenderPeriodStart": "2022-01-01",
                    "TenderPeriodEnd": "2022-01-31",
                    "Language": "en",
                    "ocid": "OCD001",
                }
            ],
            [
                {"block": "buyer", "path": "/buyer/id", "mapping": "BuyerID", "title": "Buyer ID"},
                {"block": "buyer", "path": "/buyer/name", "mapping": "BuyerName", "title": "Buyer Name"},
                {"block": "tender", "path": "/tender/id", "mapping": "TenderID", "title": "Tender ID"},
                {"block": "tender", "path": "/tender/title", "mapping": "TenderTitle", "title": "Tender Title"},
                {
                    "block": "tender",
                    "path": "/tender/tenderPeriod/startDate",
                    "mapping": "TenderPeriodStart",
                    "title": "Start Date",
                },
                {
                    "block": "tender",
                    "path": "/tender/tenderPeriod/endDate",
                    "mapping": "TenderPeriodEnd",
                    "title": "End Date",
                },
                {"block": "", "path": "/language", "mapping": "Language", "title": "Language"},
            ],
            {
                "/buyer/id": {"type": "string"},
                "/buyer/name": {"type": "string"},
                "/tender/id": {"type": "string"},
                "/tender/title": {"type": "string"},
                "/tender/tenderPeriod/startDate": {"type": "string"},
                "/tender/tenderPeriod/endDate": {"type": "string"},
                "/language": {"type": "string"},
            },
            ["/tender/tenderPeriod"],
            {
                "buyer": {"id": "B123", "name": "Dept of Education"},
                "tender": {
                    "id": "T456",
                    "title": "Supply of textbooks",
                    "tenderPeriod": {"startDate": "2022-01-01", "endDate": "2022-01-31"},
                },
                "language": "en",
            },
        ),
        (
            [
                {
                    "ContractID": "C001",
                    "MilestoneID": "M001",
                    "MilestoneTitle": "Approval",
                    "MilestoneDue": "2022-03-01",
                    "ocid": "OC001",
                },
                {
                    "ContractID": "C001",
                    "MilestoneID": "M002",
                    "MilestoneTitle": "Delivery",
                    "MilestoneDue": "2022-06-01",
                    "ocid": "OC001",
                },
            ],
            [
                {"block": "contracts", "path": "/contracts/id", "mapping": "ContractID", "title": "Contract ID"},
                {
                    "block": "contracts/milestones",
                    "path": "/contracts/milestones/id",
                    "mapping": "MilestoneID",
                    "title": "Milestone ID",
                },
                {
                    "block": "contracts/milestones",
                    "path": "/contracts/milestones/title",
                    "mapping": "MilestoneTitle",
                    "title": "Milestone Title",
                },
                {
                    "block": "contracts/milestones",
                    "path": "/contracts/milestones/dueDate",
                    "mapping": "MilestoneDue",
                    "title": "Milestone Due Date",
                },
            ],
            {
                "/contracts/id": {"type": "string"},
                "/contracts/milestones": {"type": "array"},
                "/contracts/milestones/id": {"type": "string"},
                "/contracts/milestones/title": {"type": "string"},
                "/contracts/milestones/dueDate": {"type": "string"},
            },
            ["/contracts/milestones"],
            {
                "contracts": {
                    "id": "C001",
                    "milestones": [
                        {"id": "M001", "title": "Approval", "dueDate": "2022-03-01"},
                        {"id": "M002", "title": "Delivery", "dueDate": "2022-06-01"},
                    ],
                }
            },
        ),
        (
            [
                {"ContactID": "C100", "MetricID": "M200", "ObsID": "O300", "ObsAmount": "1000", "ocid": "OC002"},
                {"ContactID": "C100", "MetricID": "M200", "ObsID": "O301", "ObsAmount": "2000", "ocid": "OC002"},
            ],
            [
                {"block": "contacts", "path": "/contacts/id", "mapping": "ContactID", "title": "Contact ID"},
                {"block": "contacts", "path": "/contacts/name", "mapping": "ContactName", "title": "Contact Name"},
                {
                    "block": "contacts/agreedMetrics",
                    "path": "/contacts/agreedMetrics/id",
                    "mapping": "MetricID",
                    "title": "Metric ID",
                },
                {
                    "block": "contacts/agreedMetrics",
                    "path": "/contacts/agreedMetrics/title",
                    "mapping": "MetricTitle",
                    "title": "Metric Title",
                },
                {
                    "block": "contacts/agreedMetrics/observations",
                    "path": "/contacts/agreedMetrics/observations/id",
                    "mapping": "ObsID",
                    "title": "Observation ID",
                },
                {
                    "block": "contacts/agreedMetrics/observations",
                    "path": "/contacts/agreedMetrics/observations/value/amount",
                    "mapping": "ObsAmount",
                    "title": "Observation Amount",
                },
            ],
            {
                "/contacts/id": {"type": "string"},
                "/contacts/name": {"type": "string"},
                "/contacts/agreedMetrics": {"type": "object"},
                "/contacts/agreedMetrics/id": {"type": "string"},
                "/contacts/agreedMetrics/title": {"type": "string"},
                "/contacts/agreedMetrics/observations": {"type": "array"},
                "/contacts/agreedMetrics/observations/id": {"type": "string"},
                "/contacts/agreedMetrics/observations/value/amount": {"type": "string"},
            },
            ["/contacts/agreedMetrics/observations"],
            {
                "contacts": {
                    "id": "C100",
                    "agreedMetrics": {
                        "id": "M200",
                        "observations": [
                            {"id": "O300", "value": {"amount": "1000"}},
                            {"id": "O301", "value": {"amount": "2000"}},
                        ],
                    },
                }
            },
        ),
        (
            [
                {"SupplierName": "Acme Supplies", "SupplierID": "ACME-001", "ocid": "OC003"},
                {"SupplierName": "Beta Inc", "SupplierID": "BETA-002", "ocid": "OC003"},
            ],
            [
                {
                    "block": "parties",
                    "path": "/parties/name",
                    "mapping": "SupplierName",
                    "title": "Party Supplier Name",
                },
                {"block": "parties", "path": "/parties/id", "mapping": "SupplierID", "title": "Party Supplier ID"},
                {
                    "block": "awards",
                    "path": "/awards/suppliers/name",
                    "mapping": "SupplierName",
                    "title": "Award Supplier Name",
                },
                {
                    "block": "awards",
                    "path": "/awards/suppliers/id",
                    "mapping": "SupplierID",
                    "title": "Award Supplier ID",
                },
            ],
            {
                "/parties": {"type": "array"},
                "/parties/name": {"type": "string"},
                "/parties/id": {"type": "string"},
                "/awards/suppliers": {"type": "array"},
                "/awards/suppliers/name": {"type": "string"},
                "/awards/suppliers/id": {"type": "string"},
            },
            ["/parties", "/awards/suppliers"],
            {
                "parties": [{"name": "Acme Supplies", "id": "ACME-001"}, {"name": "Beta Inc", "id": "BETA-002"}],
                "awards": {
                    "suppliers": [{"name": "Acme Supplies", "id": "ACME-001"}, {"name": "Beta Inc", "id": "BETA-002"}]
                },
            },
        ),
        (
            [{"BuyerRole": "buyer", "ocid": "OC004"}, {"SupplierRole": "supplier", "ocid": "OC004"}],
            [
                {"block": "parties", "path": "/parties/roles", "mapping": "BuyerRole", "title": "Party Role"},
                {"block": "parties", "path": "/parties/roles", "mapping": "SupplierRole", "title": "Party Role"},
            ],
            {"/parties/roles": {"type": "array"}, "/parties": {"type": "array"}},
            ["/parties", "/parties/roles"],
            {"parties": [{"roles": ["buyer", "supplier"]}]},
        ),
        (
            [
                {
                    "TenderId": "1",
                    "ocid": "OC005",
                    "CritType1": "Technical",
                    "CritDesc1": "Must have relevant experience",
                    "CritType2": "Economic",
                    "CritDesc2": "Lowest bid wins",
                }
            ],
            [
                {"block": "tender", "path": "/tender/id", "mapping": "TenderID", "title": "Tender ID"},
                {
                    "block": "tender",
                    "path": "/tender/selectionCriteria/criteria/type",
                    "mapping": "CritType1",
                    "title": "Criterion Type",
                },
                {
                    "block": "tender",
                    "path": "/tender/selectionCriteria/criteria/description",
                    "mapping": "CritDesc1",
                    "title": "Criterion Description",
                },
                {
                    "block": "tender",
                    "path": "/tender/selectionCriteria/criteria/type",
                    "mapping": "CritType2",
                    "title": "Criterion Type",
                },
                {
                    "block": "tender",
                    "path": "/tender/selectionCriteria/criteria/description",
                    "mapping": "CritDesc2",
                    "title": "Criterion Description",
                },
            ],
            {
                "/tender/id": {"type": "string"},
                "/tender/selectionCriteria/criteria": {"type": "array"},
                "/tender/selectionCriteria/criteria/type": {"type": "string"},
                "/tender/selectionCriteria/criteria/description": {"type": "string"},
            },
            ["/tender/selectionCriteria/criteria"],
            {
                "tender": {
                    "selectionCriteria": {
                        "criteria": [
                            {"type": "Technical", "description": "Must have relevant experience"},
                            {"type": "Economic", "description": "Lowest bid wins"},
                        ]
                    }
                }
            },
        ),
    ],
    ids=[
        "BasicRelease",
        "ContractMilestones",
        "DeepAgreedMetrics",
        "SupplierMapping",
        "PartiesRoles",
        "TenderCriteria",
    ],
)
def test_transform_data_arrays(rows, mapping_items, schema_def, array_paths, expected_output, base_config):
    dummy_config = dummy_ocds_mapping_config(mapping_items, array_paths)
    dummy_template = dummy_ocds_mapping_template(dummy_config, schema_def)
    with mock.patch("nightingale.mapper.MappingTemplate", return_value=dummy_template):
        mapper = OCDSDataMapper(base_config)
    mapper.mapping = dummy_template
    result = {}
    for row in rows:
        result = mapper.transform_row(row, dummy_template, schema_def, result=result, array_counters={})
    assert result == expected_output


@pytest.mark.parametrize(
    "input_ocid, expected_ocid",
    [
        ("12345", "prefix-12345"),
        ("abcde", "prefix-abcde"),
        ("67890", "prefix-67890"),
    ],
    ids=["ID1", "ID2", "ID3"],
)
@mock.patch(
    "nightingale.mapper.MappingTemplate", return_value=dummy_ocds_mapping_template(dummy_ocds_mapping_config([]), {})
)
def test_produce_ocid(mock_mapping_template, base_config, input_ocid, expected_ocid):
    mapper = OCDSDataMapper(base_config)
    assert mapper.produce_ocid(input_ocid) == expected_ocid


@mock.patch(
    "nightingale.mapper.MappingTemplate", return_value=dummy_ocds_mapping_template(dummy_ocds_mapping_config([]), {})
)
def test_date_release(mock_config, base_config):
    mapper = OCDSDataMapper(base_config)
    curr_row = {}
    mapper.date_release(curr_row, "2022-01-01T00:00:00Z")
    assert curr_row.get("date") == "2022-01-01T00:00:00Z"


@mock.patch(
    "nightingale.mapper.MappingTemplate", return_value=dummy_ocds_mapping_template(dummy_ocds_mapping_config([]), {})
)
def test_tag_initiation_type(mock_config, base_config):
    mapper = OCDSDataMapper(base_config)
    curr_row = {"tender": {}}
    mapper.tag_initiation_type(curr_row)
    assert curr_row.get("initiationType") == "tender"


@mock.patch(
    "nightingale.mapper.MappingTemplate", return_value=dummy_ocds_mapping_template(dummy_ocds_mapping_config([]), {})
)
def test_tag_ocid(mock_config, base_config):
    mapper = OCDSDataMapper(base_config)
    curr_row = {}
    mapper.tag_ocid(curr_row, "12345")
    assert curr_row.get("ocid") == "prefix-12345"


@mock.patch(
    "nightingale.mapper.MappingTemplate", return_value=dummy_ocds_mapping_template(dummy_ocds_mapping_config([]), {})
)
def test_remove_empty_id_arrays(mock_config, base_config):
    mapper = OCDSDataMapper(base_config)
    input_data = {
        "id": "1",
        "tender": {
            "items": [{"id": "item1", "description": "Item 1"}, {"description": "Item without ID"}],
            "documents": [{"id": "doc1", "title": "Document 1"}, {"title": "Document without ID"}],
        },
    }
    expected_output = {
        "id": "1",
        "tender": {
            "items": [{"id": "item1", "description": "Item 1"}],
            "documents": [{"id": "doc1", "title": "Document 1"}],
        },
    }
    data = mapper.remove_empty_id_arrays(input_data)
    assert data == expected_output


def generate_hash(data):
    return dict_hash.sha256(data)


@mock.patch("nightingale.mapper.get_iso_now")
@mock.patch(
    "nightingale.mapper.MappingTemplate", return_value=dummy_ocds_mapping_template(dummy_ocds_mapping_config([]), {})
)
def test_finish_release(mock_config, mock_get_iso_now, base_config):
    mock_get_iso_now.return_value = "2022-01-01T00:00:00Z"
    mapper = OCDSDataMapper(base_config)
    curr_release = {"field": "value1", "tender": {"id": 1}}
    curr_ocid = "1"
    mapped = []
    mapper.finish_release(curr_ocid, curr_release, mapped, None)
    expected_release = {
        "field": "value1",
        "initiationType": "tender",
        "date": "2022-01-01T00:00:00Z",
        "ocid": "prefix-1",
        "tag": ["tender"],
        "tender": {"id": 1},
        "id": generate_hash(
            {
                "field": "value1",
                "initiationType": "tender",
                "date": "2022-01-01T00:00:00Z",
                "ocid": "prefix-1",
                "tag": ["tender"],
                "tender": {"id": 1},
            }
        ),
    }
    assert len(mapped) == 1
    assert mapped[0] == expected_release


@mock.patch("nightingale.mapper.get_iso_now")
@mock.patch(
    "nightingale.mapper.MappingTemplate", return_value=dummy_ocds_mapping_template(dummy_ocds_mapping_config([]), {})
)
def test_finish_release_with_tags(mock_mapping_template, mock_get_iso_now, base_config):
    mock_get_iso_now.return_value = "2022-01-01T00:00:00Z"
    mapper = OCDSDataMapper(base_config)
    mapper.transform_row = mock.Mock(
        side_effect=[
            {"ocid": "1", "field": "value1", "tender": {"id": "tender", "amendments": [{"id": "one"}]}},
            {
                "ocid": "2",
                "field": "value2",
                "tender": {"id": "tender"},
                "awards": [{"id": "award"}],
                "contracts": [{"id": "contract"}],
            },
            {
                "ocid": "3",
                "field": "value3",
                "contracts": [
                    {
                        "id": "contract",
                        "implementation": {"transactions": [{"id": "transaction"}]},
                        "amendments": [{"id": "one"}],
                    }
                ],
            },
        ]
    )
    result = mapper.transform_data([{"ocid": 1}, {"ocid": 2}, {"ocid": 3}], mapper.mapping)
    assert result[0]["tag"] == ["tender", "tenderAmendment"]
    assert result[1]["tag"] == ["tender", "award", "contract"]
    assert result[2]["tag"] == ["contract", "contractAmendment", "implementation"]


if __name__ == "__main__":
    pytest.main()
