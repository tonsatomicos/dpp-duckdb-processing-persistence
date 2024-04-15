# <p align="center">Projeto de Processamento de Dados<br>Explorando DuckDB</p>

<p align="center">
<img src="http://img.shields.io/static/v1?label=LICENCA&message=...&color=GREEN&style=for-the-badge"/>     
<img src="http://img.shields.io/static/v1?label=STATUS&message=N/A&color=GREEN&style=for-the-badge"/>
</p>

Este projeto foi concebido como parte de um desafio durante a disciplina de Linguagem de Programação para Engenharia de Dados, no curso de pós-graduação em Engenharia de Dados, na Universidade de Fortaleza (Unifor).

O desafio consistia em lidar com um extenso volume de dados fornecidos pelo Instituto Brasileiro de Geografia e Estatística (IBGE), no formato CSV, hospedados no Google Drive, sem depender de bibliotecas como Pandas e Spark. Optei por automatizar o processo de extração desses arquivos CSV para eliminar a necessidade de intervenção manual.

Para a leitura, processamento e persistência dos dados, escolhi utilizar o DuckDB. Os dados foram armazenados em um banco de dados PostgreSQL, o qual foi dockerizado para simplificar o gerenciamento do ambiente.

É importante salientar que os dados utilizados neste desafio estão disponíveis exclusivamente no Google Drive para os alunos da disciplina. No entanto, o projeto pode ser facilmente adaptado para outros contextos nos quais os dados estejam disponíveis.

## Diagrama de Fluxo

![Diagram](https://github.com/tonsatomicos/dpp-duckdb-processing-persistence/blob/main/assets/diagram_pipeline.png?raw=true)

Sinta-se à vontade para clonar, adaptar e ajustar o projeto conforme necessário. Consulte as instruções abaixo, se precisar. :alien:

## Dependências do Projeto

Este projeto foi desenvolvido utilizando o Poetry + Pyenv para gerenciamento de ambientes virtuais e bibliotecas.

### Bibliotecas Utilizadas

- duckdb (v0.10.1)
- jupyter (v1.0.0)
- google-auth (v2.28.2)
- google-auth-oauthlib (v1.2.0)
- google-auth-httplib2 (v0.2.0)
- google-api-python-client (v2.122.0)
- python-dotenv (v1.0.1)

### Instalação das Dependências

Você pode instalar as dependências manualmente, ou, utilizando o Poetry ou o Pip com os seguintes comandos:

#### Utilizando Poetry

```bash
poetry config virtualenvs.in-project true
pyenv local 3.12.1
poetry env use 3.12.1
poetry install

```

#### Utilizando Pip

```bash
pip install -r requirements.txt

```

## Configurações do Projeto - Parte 1

- Acesso ao Google Cloud Console: **<a href="https://console.cloud.google.com">console.cloud.google.com</a>**.
- Conta de e-mail para configuração de permissões.

### Passos

- Acesse o Google Cloud Console utilizando o link fornecido acima.
- Crie um novo projeto no menu principal.
- No menu lateral, navegue até "APIs e Serviços" > "Tela de Permissão OAuth" e adicione o seu endereço de e-mail como um usuário de teste.
- Volte ao menu principal e selecione "Credenciais".
- Crie um novo conjunto de credenciais selecionando "IDs do Cliente OAuth".
- Baixe o arquivo de credenciais gerado e salve-o como credentials.json, na pasta <code>config/credentials</code>.
- Na barra de pesquisa na parte superior da página, procure por "Google Drive API" e clique para ativar o serviço.</li>
- Crie um arquivo <code>.env</code> na pasta <code>config</code> e salve nele a seguinte linha:<pre><code>FOLDER_ID=id_da_pasta_google_drive</code></pre>
- Substitua <code>id_da_pasta_google_drive</code> pelo ID da pasta do Google Drive que você deseja acessar para fazer o download dos arquivos.
- O primeiro acesso irá abrir uma tela do Google para autenticação.

### Conclusão

Após seguir esses passos, você estará pronto para automatizar o download dos arquivos do Google Drive. No entanto, esteja ciente de que este tutorial é uma orientação geral e pode precisar ser ajustado de acordo com as especificidades do seu projeto ou ambiente.

## Configurações do Projeto - Parte 2

E é aqui que iniciamos a etapa de leitura, processamento e persistência dos dados. Após definirmos a estrutura da tabela temporária que receberá as informações do CSV com DuckDB, podemos prosseguir com as configurações do banco de dados PostgreSQL, onde persistiremos as informações.

### Banco de dados PostgreSQL

Você pode escolher entre utilizar o Docker para subir um banco PostgreSQL ou instalar de outras maneiras.

#### Utilizando Docker

<pre><code>docker-compose up -d</code></pre>

#### Outras maneiras

Segue tutorial aleatório da **<a href="https://youtu.be/L_2l8XTCPAE?si=-OJ21qv_48BgHFq2">Hashtag Treinamentos</a>**. <br>Script de criação da tabela disponibilizado em <code>src/sql</code>.

### Conclusão

Lembre-se sempre de verificar o usuário, senha, base e porta. Procure na estrutura o seguinte código, localizado na função <code>processing_persistence</code>, e altere se necessário.

<pre><code>conn.execute("ATTACH 'dbname=unifor_duckdb user=unifor password=unifor host=localhost port=5437' AS db (TYPE postgres)")</code></pre>

Isso irá garantir que as informações sejam persistidas no banco de dados PostgreSQL.</p>


## Considerações Finais

- Os arquivos CSV não estão disponíveis nas pastas <code>raw</code> e <code>processed</code> porque são muito pesados.
- O código foi adaptado para lidar com erros, ou pelo menos auxiliar na compreensão deles, e para evitar o download de arquivos que já foram baixados anteriormente, etc.
- Também inclui a "boa prática" de salvar o nome do arquivo na tabela, juntamente com a data e hora de processamento.
- É notável o quão rápido o DuckDB conseguiu ler, processar e persistir mais de 100 milhões de linhas em minha máquina, levando cerca de 2 minutos para persistir, utilizando 24 GB de RAM. Surge um forte concorrente para o Spark? Veremos nos próximos episódios. 

<hr>

![Image](https://i.imgur.com/p4vnGAN.gif)
