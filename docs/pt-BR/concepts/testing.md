# Testes com o kilntest

`kilntest.Work` conduz um job pela cadeia de middleware real e já montada, e pela lógica de
classificação, sem precisar de um servidor, para testes unitários de handlers; o resultado inclui as
linhas do console e o progresso. `RequireEnqueued`/`RequireNotEnqueued` verificam o que um trecho de
código de fato enfileirou. `drivertest.Run` é a suíte de conformidade que uma implementação de
`driver.Store` precisa passar.
