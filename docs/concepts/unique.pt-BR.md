# Jobs únicos

`Unique{Key}` faz `Enqueue` retornar o id de um job ativo com o mesmo tipo e a mesma chave, em vez de
inserir outro. A chave é liberada assim que esse job é concluído, falha ou é excluído.

`Unique{Key, For: d}` segura a chave por `d` a partir do primeiro enfileiramento, não importa o que
aconteça com o job nesse meio-tempo, incluindo sucesso. Isso significa "no máximo uma vez a cada `d`"
(um lembrete a cada 10 minutos), não "sem duplicatas enquanto ele roda".

`Unique{Key, Replace: true}` atualiza um detentor que ainda não começou com os novos args, metadados,
tags, título e prioridade, então o job roda com os dados mais recentes. `Unique{Key, Debounce: d}` roda
o job `d` depois do último enfileiramento: cada novo enfileiramento empurra um detentor que ainda está
esperando e substitui seus args, o que serve para trabalhos como "reindexar quando o usuário parar de
editar". Um detentor que já está rodando nunca é alterado.
