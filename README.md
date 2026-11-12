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

## Executar

```powershell
dotnet run
```

Configuracao padrao: MongoDB em `localhost:27017` e RabbitMQ em `localhost:5672`.
