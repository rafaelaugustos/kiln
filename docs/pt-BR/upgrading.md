# Atualizando

Toda release é testada contra a anterior no mesmo banco de dados: o módulo `compat` roda a versão
publicada e o código novo lado a lado enquanto jobs se movem entre eles. Mudanças de schema só
adicionam coisas, nunca removem, então servidores em duas versões consecutivas podem rodar juntos
durante um rolling deploy.

A v0.3 adiciona as colunas de rate limit. Durante um rolling deploy a partir da v0.2, jobs em uma chave
com `Rate` e sem `Max` só são liberados por servidores já rodando v0.3, no ritmo que a taxa permite; uma
vez liberados, qualquer servidor pode executá-los. Em uma chave com os dois, servidores v0.2 ainda
aplicam o `Max`, mas não a taxa, até serem atualizados. Todo o resto é processado pelas duas versões.

Mudanças aditivas são registradas em uma tabela `schema_changes`, ao lado das tabelas de jobs. Se a role
do banco de dados com que o kiln roda recebeu privilégios tabela por tabela, conceda os mesmos na
`schema_changes` depois da atualização.

A v0.4 adiciona uma coluna `admit_tat` anulável à tabela de limites. Até que todo servidor rode v0.4, os
que ainda estão na v0.3 liberam jobs cujo horário de início já chegou sem a verificação que evita que
eles comecem juntos depois de uma pausa.

A v0.5 não tem mudanças de schema para os stores existentes. `mssqlstore` é novo nela.

A v0.6 adiciona duas mudanças, as duas aditivas: uma tabela de log e uma coluna `progress` para o
console do job (`003_console`), e uma coluna de grupo anulável nos jobs recorrentes
(`004_recurring_group`). Na edição Standard do SQL Server, adicionar a coluna `progress` com seu default
pode tocar em toda linha das tabelas de jobs e de archive; nas edições Enterprise, Developer e Azure SQL,
isso só muda metadados.

A v0.7 adiciona uma mudança aditiva, `005_job_extras`: uma coluna `title` anulável nas tabelas de jobs e
de archive.

A v0.8 adiciona uma mudança aditiva, `006_nested_batches`: uma coluna `parent_id` anulável em lotes e
dois índices. Servidores v0.7 não sabem da existência de lotes aninhados e podem terminar um lote
enquanto lotes aninhados nele ainda estão rodando, então só aninhe lotes depois que todo servidor rodar
v0.8.

A v0.8.1 e a v0.8.2 não têm mudanças de schema.

A v0.9 também não tem mudanças de schema. `Client.OpenBatch` agora recebe um `*Batch`, assim como
`StartBatch`: troque `OpenBatch(ctx, desc, meta)` por
`OpenBatch(ctx, &kiln.Batch{Description: desc, Meta: meta})`.
