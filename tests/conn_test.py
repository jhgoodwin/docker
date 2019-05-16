from pg_utils import connect
import pytest
import pytest_asyncio
import asyncio
import logging

@pytest.mark.asyncio
async def test_basic_connection(citus_master_postgres_url: str):
    set_shard_count(citus_master_postgres_url, 120)
    await asyncio.gather(
        create_test_distribute_table(citus_master_postgres_url, f'test_table1'),
        create_test_distribute_table(citus_master_postgres_url, f'test_table2'),
        create_test_distribute_table(citus_master_postgres_url, f'test_table3'),
        create_test_distribute_table(citus_master_postgres_url, f'test_table4'),
        create_test_distribute_table(citus_master_postgres_url, f'test_table5'),
        create_test_distribute_table(citus_master_postgres_url, f'test_table6'),
        create_test_distribute_table(citus_master_postgres_url, f'test_table7'),
        create_test_distribute_table(citus_master_postgres_url, f'test_table8'),
        create_test_distribute_table(citus_master_postgres_url, f'test_table9'),
        create_test_distribute_table(citus_master_postgres_url, f'test_table10'),
        create_test_distribute_table(citus_master_postgres_url, f'test_table11'),
        create_test_distribute_table(citus_master_postgres_url, f'test_table12'),
        create_test_distribute_table(citus_master_postgres_url, f'test_table13'),
        create_test_distribute_table(citus_master_postgres_url, f'test_table14'),
        create_test_distribute_table(citus_master_postgres_url, f'test_table15'),
        create_test_distribute_table(citus_master_postgres_url, f'test_table16'),
        create_test_distribute_table(citus_master_postgres_url, f'test_table17'),
        create_test_distribute_table(citus_master_postgres_url, f'test_table18'),
        create_test_distribute_table(citus_master_postgres_url, f'test_table19'),
        create_test_distribute_table(citus_master_postgres_url, f'test_table20'),
        create_test_distribute_table(citus_master_postgres_url, f'test_table21')
    )
    assert True

def set_shard_count(citus_master_postgres_url: str, shard_count: int):
    with connect(citus_master_postgres_url) as cur:
        logging.debug(f'Setting citus.shard_count to {shard_count}')
        cur.execute(f'SET citus.shard_count={shard_count}')
        cur.execute(f'SHOW citus.shard_count')
        result = cur.fetchone()
        logging.debug(f'Set citus.shard_count returned {result}')
        logging.debug(f'Set citus.shard_count to {shard_count}')
        return result

async def create_test_distribute_table(citus_master_postgres_url: str, table_name: str):
    with connect(citus_master_postgres_url) as cur:
        logging.debug(f'Creating table {table_name}')
        cur.execute("""
        CREATE TABLE xxxTablexxx (
            distribution_id serial PRIMARY KEY,
            value TEXT NOT NULL
        );
        SELECT create_distributed_table('xxxTablexxx', 'distribution_id');
        INSERT INTO xxxTablexxx (value) VALUES ('fred');
        SELECT distribution_id, value FROM xxxTablexxx;
        """.replace("xxxTablexxx", table_name))
        result = cur.fetchone()
        cur.execute("""
        DROP TABLE xxxTablexxx;
        """.replace("xxxTablexxx", table_name))

        logging.debug(f'Creating table {table_name} returned {result}')
        logging.debug(f'Created table {table_name}')
