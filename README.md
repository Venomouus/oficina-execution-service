# Oficina Execution Service

Microsserviço responsável por receber ordens pagas e controlar sua fila de execução.

## Stack

- ASP.NET Core 8
- MongoDB exclusivo do serviço
- RabbitMQ para integração assíncrona
- Swagger/OpenAPI

## Eventos

- Consome `PAYMENT_APPROVED`.
- Registra a OS com status `NA_FILA`.

## Executar

```powershell
dotnet run
```

Configuração padrão: MongoDB em `localhost:27017` e RabbitMQ em `localhost:5672`.
