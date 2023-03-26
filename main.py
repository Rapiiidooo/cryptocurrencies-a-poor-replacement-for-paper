import pandas


def parse_eth():
    df = pandas.read_csv('histo-ether.csv')
    df['date'] = pandas.to_datetime(df['date'], format="%m/%d/%Y")
    df_sum = df.groupby(df.date.dt.year)['value'].sum().reset_index(name='ethereum')
    return df_sum

def parse_btc():
    import json

    with open('histo-bitcoin.json') as f:
        data = dict()
        json_data = json.load(f)

        for row in json_data["dataset"]["data"]:
            data[row[0]] = row[1]

    df = pandas.DataFrame(data.items(), columns=['date', 'value'])
    df['date'] = pandas.to_datetime(df['date'], format="%Y-%m-%d")
    df_sum = df.groupby(df.date.dt.year)['value'].sum().reset_index(name='bitcoin')
    return df_sum


def parse_visa():
    df = pandas.read_csv('histo-visa.csv').rename(columns={"value": "visa"})
    return df


def parse_mastercard():
    df = pandas.read_csv('histo-mastercard.csv').rename(columns={"value": "mastercard"})
    return df


if __name__ == '__main__':
    print("TRANSACTION PER YEAR:")

    df_eth = parse_eth()
    df_btc = parse_btc()
    df_visa = parse_visa()
    df_mastercard = parse_mastercard()

    df = df_btc.merge(
        df_eth,
        on=["date"],
        how='outer',
    ).merge(
        df_visa,
        on=["date"],
        how='outer',
    ).merge(
        df_mastercard,
        on=["date"],
        how='outer',
    ).fillna(0)
    df.loc['total'] = df.sum()
    df.date = pandas.to_numeric(df.date, downcast='integer')
    df.date["total"] = 0

    print(df)
    df.to_csv('histo-global.csv', index=False)
    print("csv file saved into histo-global.csv")
