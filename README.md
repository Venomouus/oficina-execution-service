# Oficina Execution Service

Microsservico responsavel por receber ordens pagas e controlar sua fila de execucao.

## Stack

- ASP.NET Core 8
- MongoDB exclusivo do servico
- RabbitMQ para integracao assincrona
- Swagger/OpenAPI

## Eventos

- Consome `OS_READY_FOR_EXECUTION`.
- Registra a OS com status `NA_FILA`.
- Publica `EXECUTION_FINISHED` ou `EXECUTION_FAILED` para o OS Service.

## Endpoints principais

- `GET /api/execution`
- `PATCH /api/execution/{orderId}/status`

Estados aceitos: `NA_FILA`, `EM_DIAGNOSTICO`, `EM_REPARO`, `FINALIZADA` e `EXECUCAO_COM_ERRO`.
O fluxo normal e `NA_FILA -> EM_DIAGNOSTICO -> EM_REPARO -> FINALIZADA`.
Uma falha pode ser registrada em qualquer etapa nao terminal. Transicoes
invalidas retornam HTTP 409; repetir o mesmo status nao republica o evento.
O evento de finalizacao/falha e gravado no documento da execucao junto com a
mudanca de status e reenviado por um worker se o RabbitMQ estiver indisponivel.
O consumidor reconecta ao RabbitMQ e um indice unico em `OrderId` evita criar
duas execucoes para a mesma OS. Falhas temporarias aguardam 30 segundos em
`execution-service-events.retry`; mensagens malformadas ou que excedam 5
novas tentativas vao para `execution-service-events.dead` e exigem investigacao e
reenvio manual. Se o MongoDB ja contiver OS duplicadas, corrija os registros
antes de iniciar o consumidor; o servico nao apaga dados automaticamente.

## Executar

```powershell
dotnet run
```

Configuracao padrao: MongoDB em `localhost:27017` e RabbitMQ em `localhost:5672`.

## Testes e cobertura

Os testes de integracao iniciam MongoDB e RabbitMQ descartaveis com Testcontainers;
e necessario ter Docker em execucao. Nenhum teste usa os bancos ou filas locais
configurados para o servico. Em um diretorio de resultados novo, execute:

```powershell
$results = "Tests/ExecutionService.Tests/TestResults/coverage-$([guid]::NewGuid().ToString('N'))"
dotnet test Tests/ExecutionService.Tests/ExecutionService.Tests.csproj --configuration Release --collect:"XPlat Code Coverage" --results-directory $results
powershell -NoProfile -File Tests/verify-coverage.ps1 -ResultsDirectory $results
```

O verificador exige um unico relatorio Cobertura, confirma que ele cobre a
assembly de producao completa `ExecutionService` e falha abaixo de 80% das
linhas. O CI executa o mesmo teste e verificador.
O relatorio Cobertura fica disponivel no artefato `execution-service-coverage`
da execucao do workflow de CI.
O [painel SonarCloud do PR](https://sonarcloud.io/dashboard?id=Venomouus_oficina-execution-service&pullRequest=1)
mostra o quality gate e a cobertura de codigo novo; o verificador local/CI
exige separadamente pelo menos 80% de linhas da assembly de producao.

# CI/CD e observabilidade da Fase 4

O PR executa testes/cobertura, SonarCloud com Quality Gate bloqueante e build
Docker. Em `main`, `publish-deploy` assume a role AWS via OIDC, publica
`oficina-fase4-lab/execution-service:<SHA>` no ECR privado e aguarda o rollout
no EKS. Os Secrets `messaging-secrets` e `execution-service-secrets` (chave
`database-connection`) devem existir no namespace `oficina`; o manifesto nao
inclui credenciais reais.

Os logs JSON vao para stdout. Defina `OTEL_EXPORTER_OTLP_ENDPOINT` para enviar
traces, metricas HTTP/Saga e logs por OTLP gRPC ao coletor da Fase 3. No
Kubernetes, configure opcionalmente `observability-config/otlp-endpoint`.
