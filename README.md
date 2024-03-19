# <p align="center">DuckDB - Processando e Persistindo</p>

<p align="center">
<img src="http://img.shields.io/static/v1?label=LICENSE&message=...&color=GREEN&style=for-the-badge"/>     
<img src="http://img.shields.io/static/v1?label=STATUS&message=N/A&color=GREEN&style=for-the-badge"/>
</p>

**Desafio desenvolvido durante a disciplina de Linguagens de Programação para Engenharia de Dados, no curso de pós-graduação em Engenharia de Dados, na Universidade de Fortaleza (Unifor).**

**O objetivo era lidar com um grande volume de dados sem recorrer ao uso de bibliotecas como pandas e spark, e ao final persistir os dados em algum banco de dados. Os dados foram disponibilizados no formato CSV, hospedados no Google Drive.**

**Sobre esse desafio, existem muitas abordagens para resolver, mas optei por automatizar o processo de extração desses CSV para evitar que se tornasse uma atividade manual. Além disso, utilizei o DuckDB para leitura, processamento e persistência dos dados em um banco de dados PostgreSQL, o qual está dockerizado.**

## Dependências do Projeto

**Este projeto foi desenvolvido utilizando o Poetry para gerenciamento de ambientes virtuais e pacotes.**

### Pacotes Utilizados

- **duckdb** (v0.10.1)
- **jupyter** (v1.0.0)
- **google-auth** (v2.28.2)
- **google-auth-oauthlib** (v1.2.0)
- **google-auth-httplib2** (v0.2.0)
- **google-api-python-client** (v2.122.0)
- **python-dotenv** (v1.0.1)

### Instalação das Dependências

Você pode instalar as dependências utilizando o Poetry ou o Pip com os seguintes comandos:

### Utilizando Poetry

```bash
poetry add -r requirements.txt

```

### Utilizando Pip

```bash
pip install -r requirements.txt

```

## Configurações do Projeto - Parte 1

- **Acesso ao Google Cloud Console: <a href="https://console.cloud.google.com">console.cloud.google.com</a>**
- **Conta de e-mail para configuração de permissões**

### Passos

- **Acesse o Google Cloud Console utilizando o link fornecido acima.**
- **Crie um novo projeto no menu principal.**
- **No menu lateral, navegue até "APIs e Serviços" > "Tela de Permissão OAuth" e adicione o seu endereço de e-mail como um usuário de teste.**
- **Volte ao menu principal e selecione "Credenciais".**
- **Crie um novo conjunto de credenciais selecionando "IDs do Cliente OAuth".**
- **Baixe o arquivo de credenciais gerado e salve-o como credentials.json, na pasta <code>config/credentials</code>.**
- **Na barra de pesquisa na parte superior da página, procure por "Google Drive API" e clique para ativar o serviço.</li>**
- **Crie um arquivo <code>.env</code> na pasta <code>config</code> e salve nele a seguinte linha:**
<pre><code>FOLDER_ID=id_da_pasta_google_drive</code></pre>
**Substitua <code>id_da_pasta_google_drive</code> pelo ID da pasta do Google Drive que você deseja acessar para fazer o download dos arquivos.**

### Conclusão
**Após seguir esses passos, você estará pronto para automatizar o download dos arquivos do Google Drive. Esteja ciente de que este tutorial é uma orientação geral e pode precisar ser ajustado dependendo das especificidades do seu projeto.**