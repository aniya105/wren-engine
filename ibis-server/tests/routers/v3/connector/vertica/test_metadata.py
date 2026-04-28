from tests.routers.v3.connector.vertica.conftest import base_url


async def test_metadata_list_tables(client, connection_info):
    response = await client.post(
        url=f"{base_url}/metadata/tables",
        json={"connectionInfo": connection_info},
    )
    assert response.status_code == 200
    result = response.json()
    assert len(result) > 0
    table = result[0]
    assert "name" in table
    assert "columns" in table
    assert len(table["columns"]) > 0


async def test_metadata_list_constraints(client, connection_info):
    response = await client.post(
        url=f"{base_url}/metadata/constraints",
        json={"connectionInfo": connection_info},
    )
    assert response.status_code == 200
    result = response.json()
    assert isinstance(result, list)


async def test_metadata_db_version(client, connection_info):
    response = await client.post(
        url=f"{base_url}/metadata/version",
        json={"connectionInfo": connection_info},
    )
    assert response.status_code == 200
    assert len(response.text) > 0
