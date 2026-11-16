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
