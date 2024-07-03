import requests
import matplotlib.pyplot as plt
import numpy as np

url = 'https://api.cow.fi/mainnet/api/v1/solver_competition/'

auction_ids = [
    8903909,
    8903908,
    8903907,
    8903906,
    8903905,
    8903903,
    8903902,
    8903901,
    8903900,
    8903898,
    8903897,
    8903896,
    8903895,
    8903894,
    8903893,
    8903892,
    8903891,
    8903890,
    8903889,
    8903888,
    8903887,
    8903886,
    8903885,
    8903884,
    8903883,
    8903882,
    8903881,
    8903879,
    8903878,
    8903877,
    8903876,
    8903875,
    8903874,
    8903873,
    8903872,
    8903870,
    8903869,
    8903864,
    8903856,
    8903854,
    8903853,
    8903852,
    8903849,
    8903847,
    8903845,
    8903839,
    8903837,
    8903832,
    8903830,
    8903829,
    8903825,
    8903822,
]

cow_amm = '301076c36e034948a747bb61bab9cd03f62672e3'
cow_amm = 'beef5afe88ef73337e5070ab2855d37dbf5493a4'
in_auction = False
results = []
for auction_id in auction_ids:
    response = requests.get(f'{url}{auction_id}').json()
    for order in response['auction']['orders']:
        if order[66:106] == cow_amm:
            in_auction = True
            break
    count = 0
    solver = 0
    if in_auction:
        in_auction = False
        for solution in response['solutions']:
            count+=1
            for order in solution['orders']:
                if order['id'][66:106] == cow_amm:
                    solver+=1
                    break
    results.append((count,solver, auction_id))

#32 bytes then address : 20 bytes, then 4 bytes
x, y, z = [], [], []
for i in range(len(results)):
    x.append(i)
    y.append(results[i][0])
    z.append(results[i][1])
    print(f'Auction ID: {results[i][2]}, Total Solutions: {results[i][0]}, Solutions with COW AMM: {results[i][1]}')

plt.plot(x, y, label='Total Solutions')
plt.plot(x, z, label='Solutions with COW AMM')
plt.show()

