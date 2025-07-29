import pandas as pd

# Caminhos
excel_path = r'data\Book 2(Sheet1).csv'
parquet_path = r'results\SFR\sfrSFP_2307.parquet'

# Ler Excel
df = pd.read_excel(excel_path)

# Remover colunas "Unnamed"
df = df.loc[:, ~df.columns.str.contains('^Unnamed')]

# Nomes e tipos desejados com tipos pandas modernos
tipos_colunas = {
    'data': 'datetime64[ns]',
    'IP': 'string',
    'Tipo': 'string',
    'vFW': 'string',
    'Carta': 'string',
    'vHW': 'string',
    'CardTemp': 'float64',
    'Slot': 'Int16',
    'Port': 'Int16',
    'VendorPN': 'string',
    'SerialNumber': 'string',
    'vendorSpecific': 'string',
    'temperatura': 'float64',
    'voltagem': 'float64',
    'txBias': 'float64',
    'txPower': 'float64',
    'rxPower': 'float64',
    'Tx': 'float64'
}

# Manter só colunas válidas
colunas_validas = [col for col in tipos_colunas if col in df.columns]
df = df[colunas_validas]

# Substituir espaços em branco, strings vazias, 'nan', 'null', '--', etc.
df.replace(
    to_replace=[r'^\s*$', r'(?i)^nan$', r'(?i)^null$', r'^--$'],
    value=pd.NA,
    regex=True,
    inplace=True
)

# Aplicar os tipos corretos
for col, tipo in tipos_colunas.items():
    if col in df.columns:
        if tipo == 'datetime64[ns]':
            df[col] = pd.to_datetime(df[col], errors='coerce')
        elif tipo == 'string':
            df[col] = df[col].astype(pd.StringDtype())
        elif tipo == 'float64':
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('float64')
        elif tipo == 'Int16':
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int16')

# Verificação
print("\nTipos finais:")
print(df.dtypes)

print("\nValores não nulos por coluna:")
print(df.notna().sum())

# Exportar para Parquet
df.to_parquet(parquet_path, engine='pyarrow')

print("\nParquet gerado com sucesso!")