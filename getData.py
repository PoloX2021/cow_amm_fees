from dune_client.client import DuneClient
from dune_client.types import QueryParameter
from dune_client.query import QueryBase
import os
import pandas as pd
import numpy as np
from dotenv import load_dotenv
load_dotenv()


#Fetches all the data on the evolution of the states of a cow_amm from dune : the query base table
def get_cow_amm(address):
    dune = DuneClient(
        api_key=os.getenv("DUNE_API_KEY"),
        base_url="https://api.dune.com",
        request_timeout= 300
    )
    df = dune.get_latest_result_dataframe(3637567)
    df = df[df['cow_amm_address'] == address]

    df = df[['blockchain', 'cow_amm_address', 'token_1_address', 'token_2_address', 'token_1_balance', 'token_1_balance_usd', 'token_2_balance', 'token_2_balance_usd', 'time', 'token_1_transfer', 'token_2_transfer', 'tx_hash']]
    df.rename(columns={'token_1_balance': 'token0_balance', 'token_2_balance': 'token1_balance', 'token_1_balance_usd': 'token0_balance_usd', 'token_1_address' : 'token0_address', 'token_1_transfer':'token0_transfer', 'token_2_balance_usd':'token1_balance_usd', 'token_2_address' : 'token1_address', 'token_2_transfer':'token1_transfer'}, inplace=True)
    df['token0_balance_usd'] = df['token0_balance_usd'].replace('<nil>', np.nan).astype(float)
    df['token1_balance_usd'] = df['token1_balance_usd'].replace('<nil>', np.nan).astype(float)
    df['token0_transfer'] = df['token0_transfer'].replace('<nil>', np.nan).astype(float)
    df['token1_transfer'] = df['token1_transfer'].replace('<nil>', np.nan).astype(float)
    df['time'] = pd.to_datetime(df['time']).dt.floor('min').dt.tz_convert(None)
    df['provision_rebalancing'] = (df['token1_transfer'].shift(1) == 0) | (df['token0_transfer'].shift(1) == 0)
    return df

#Fetches the price feed of a token from a given starting time from dune
def get_price_feed(time, token0, token1, blockchain):
    dune = DuneClient(
        api_key=os.getenv("DUNE_API_KEY"),
        base_url="https://api.dune.com",
        request_timeout= 30000
    )
    query = QueryBase(query_id = 3726052, 
            params=[
                QueryParameter.text_type(name="token_address", value=token0),
                QueryParameter.text_type(name="blockchain", value=blockchain),
                QueryParameter.text_type(name="time", value=time)])
    df_token0 = dune.run_query_dataframe(query=query, batch_size = 100_000)
    df_token0.rename(columns={'decimals': 'decimals0', 'price': 'price0', 'contract_address': 'contract_address0'}, inplace=True)
    df_token0['price0'] = df_token0['price0'].astype(float)

    query = QueryBase(query_id = 3726052, 
            params=[
                QueryParameter.text_type(name="token_address", value=token1),
                QueryParameter.text_type(name="blockchain", value=blockchain), 
                QueryParameter.text_type(name="time", value=time)])
    df_token1 = dune.run_query_dataframe(query=query, batch_size = 100_000)
    df_token1.rename(columns={'decimals': 'decimals1', 'price': 'price1', 'contract_address': 'contract_address1'}, inplace=True)
    df_token1['price1'] = df_token1['price1'].astype(float)

    df = pd.merge(df_token0[['decimals0', 'price0', 'contract_address0', 'minute']], df_token1, on='minute', how='outer')
    df.interpolate(method='linear', inplace=True)
    df['minute'] = pd.to_datetime(df['minute']).dt.floor('min').dt.tz_convert(None)
    return df


#Merges the data from the cow_amm and the price feed
def data(cow_amm_address):
    df = get_cow_amm(cow_amm_address)
    df = pd.merge(df, get_price_feed(df['time'].iloc[0], df['token0_address'].iloc[0], df['token1_address'].iloc[0], df['blockchain'].iloc[0]), left_on='time', right_on='minute', how='outer')
    df['token0_balance'] = df['token0_balance_usd']/df['price0']
    df['token1_balance'] = df['token1_balance_usd']/df['price1']
    df.ffill(inplace=True)
    df['token0_balance_usd'] = df['token0_balance']*df['price0']
    df['token1_balance_usd'] = df['token1_balance']*df['price1']
    return df

"""
cow_amm_address = '0x301076c36e034948a747bb61bab9cd03f62672e3' #WETH/USDC
#cow_amm_address = '0x1c7828dadade12a848f36be8e2d3146462abff68' #GNO/OLAS
#cow_amm_address = '0xbeef5afe88ef73337e5070ab2855d37dbf5493a4' #COW/WETH
df = data(cow_amm_address)
"""