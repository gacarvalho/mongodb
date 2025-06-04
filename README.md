🧭 ♨️ COMPASS
---

<p align="left">
  <img src="https://img.shields.io/badge/projeto-Compass-blue?style=flat-square" alt="Projeto">
  <img src="https://img.shields.io/badge/versão-1.0.1-blue?style=flat-square" alt="Versão">
  <img src="https://img.shields.io/badge/status-deployed-green?style=flat-square" alt="Status">
  <img src="https://img.shields.io/badge/autor-Gabriel_Carvalho-lightgrey?style=flat-square" alt="Autor">
</p>

Essa aplicação faz parte do projeto **compass-deployment** que é uma solução desenvolvida no contexto do programa Data Master, promovido pela F1rst Tecnologia, com o objetivo de disponibilizar uma plataforma robusta e escalável para captura, processamento e análise de feedbacks de clientes do Banco Santander.


![<data-master-compass>](https://github.com/gacarvalho/repo-spark-delta-iceberg/blob/main/header.png?raw=true)



`📦 artefato` `iamgacarvalho/dmc-app-ingestion-reviews-mongodb-hdfs-compass`

- **Versão:** `1.0.1`
- **Repositório:** [GitHub](https://github.com/gacarvalho/mongodb/)
- **Imagem Docker:** [Docker Hub](https://hub.docker.com/repository/docker/iamgacarvalho/dmc-app-ingestion-reviews-mongodb-hdfs-compass/tags/1.0.1/sha256-4b406055b4cabd7b2b2e5395eb6f7f1062f104f8080a2bef5d25f2c350bdf43f)
- **Descrição:**  Coleta avaliações de clientes nos canais Santander via base de dados MongoDB no ambiente interno da instituição, realizando a ingestão e os armazenando no **HDFS** em formato **Parquet**.
- **Parâmetros:**

    - `$CONFIG_ENV` (Pre, Pro) → Define o ambiente: Pre (Pré-Produção), Pro (Produção).
    - `$PARAM1` (nome-do-canal-ou-app). → Nome do canal/app no MongoDB. Para novos, use hífen (-).
    - `$PARAM2` (pf,pj). → Indicador do segmento do cliente. PF (Pessoa Física), PJ (Pessoa Juridica)
 

| Componente          | Descrição                                                                            |
|---------------------|--------------------------------------------------------------------------------------|
| **Objetivo**        | Coletar, processar e armazenar avaliações dos canais da Instituição Santander        |
| **Entrada**         | ID do app, nome do app, tipo de cliente, ambiente (pre/prod)                         |
| **Saída**           | Dados válidos/inválidos em Parquet + métricas no Elasticsearch                       |
| **Tecnologias**     | PySpark, Elasticsearch, Parquet, SparkMeasure                                        |
| **Fluxo Principal** | 1. Coleta API → 2. Validação → 3. Separação → 4. Armazenamento                       |
| **Validações**      | Duplicatas, nulos em campos críticos, consistência de tipos                          |
| **Particionamento** | Por data referencia de carga (odate)                                                 |
| **Métricas**        | Tempo execução, memória, registros válidos/inválidos, performance Spark              |
| **Tratamento Erros**| Logs detalhados, armazenamento separado de dados inválidos                           |
| **Execução**        | `spark-submit repo_extc_mongodb.py <env> <nome_da_colecao> <tipo_cliente(pf_pj)` |
