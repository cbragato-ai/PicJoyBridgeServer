# PicJoyBridgeServer

Bridge HTTP/WebSocket entre PicJoyUploader e PicJoyKioskApp.

- Desenvolvimento: `npm run dev`.
- Compilação TypeScript: `npm run build`.
- Testes de upload: `npm run test:upload` (inclui WebSockets locais; não usa banco ou serviços reais).

## Contenção do upload — protocolo 1 (legado)

O cliente pode enviar `protocolVersion: 1` no `register`. A resposta `registered` confirma a versão e informa `uploadLimits`: `maxFiles: 100`, `maxFileBytes: 20971520`, `maxTotalBytes: 2147483648`, `maxChunkChars: 65536`.

Para web registrado nessa versão, o Bridge contabiliza o lote por conexão, sem guardar conteúdo das fotos. Valida `file-meta` (`fileName`, `size`, `totalBase64Length`), `file-chunk` (`fileName`, `seq`, `chunkBase64`) e `file-complete`. Os nomes precisam ser únicos; sequências começam em zero e são contíguas. A finalização exige o tamanho Base64 declarado. `notify` com `message: 'transfer_complete'` exige todos os arquivos completos. Repetir o registro do mesmo socket não reinicia os contadores. Só uma nova conexão inicia outro lote; retomada e idempotência ficam para a etapa 2.

Em rejeições, envia `transfer-error` com `protocolVersion`, `code` e `message` ao web e ao Kiosk da sessão, e fecha somente o web com código 1008. Um erro enviado pelo Kiosk é encaminhado ao web da mesma sessão como `KIOSK_UPLOAD_ERROR`. Não é um ACK de entrega. Frames WebSocket acima de 128 KiB são recusados pelo transporte, antes do parse JSON, e podem encerrar a conexão sem uma mensagem de erro da aplicação.

Clientes sem versão mantêm o encaminhamento legado. Não recebem a garantia dos limites de lote no Bridge; um Kiosk atualizado também valida o recebimento. Versões desconhecidas são recusadas. Publicar primeiro Bridge, depois Kiosk e por último Uploader.

Existe um único registro por socket. Erros de conexão ou de `send()` removem e terminam o socket afetado, com limpeza idempotente. O protocolo 1 não confirma entrega. O controle de saída descrito abaixo também protege conexões legadas: exceder o orçamento pode encerrá-las. Não comprova capacidade de 100 fotos em produção.

## Entrega confirmada — protocolo 2

`register` admite `protocolVersion: 2`. O Kiosk abre uma nova permissão por `unlock` da própria sessão; recebe `upload-permission` com ticket aleatório de uso único e expiração de 10 minutos. O QR inclui esse ticket. O `GET /session/:code/:customerId` existente exige `uploadTicket` quando o Kiosk usa v2, antes de alterar o SQLite. O registro web apresenta o mesmo ticket e `id` igual ao `customerId`. O ticket fica vinculado à geração do QR, sessão, cliente e socket; a transferência fica vinculada ao mesmo par web/Kiosk.

O fluxo usa `upload-request` (`transferId`, `requestId`, `operation`) e `upload-ack`, emitido pelo Kiosk exato. `start` contém manifesto com até 100 `fileId` distintos, nomes, MIME e bytes. `chunk` leva `fileId`, `seq`, Base64 e SHA-256; `file-complete` confirma a foto; `finish` só recebe ACK após processamento do lote. Fotos homônimas são permitidas. Um ACK do web ou de outra sessão não avança o envio.

- Um pedido por vez, até 128 KiB de JSON por sessão; blocos de até 48 KiB binários/64 KiB Base64.
- Até 32 transferências ativas, reservando 128 KiB por transferência em um orçamento global de 4 MiB. O Bridge guarda contadores, manifesto e hashes de requisições; não retém payload para reenvio.
- Repetição idêntica pendente não é encaminhada de novo. Repetição idêntica do último pedido confirmado devolve o ACK guardado. Alteração de conteúdo ou sequência inválida aborta. Não existe retomada após reconectar: reiniciar junto ao totem com novo QR.
- Timeout de pedido/inatividade de 60 s no Bridge; finalização até 610 s; lote com prazo absoluto de 30 minutos. O uploader tenta o mesmo pedido até três vezes, a cada 15 s, e aguarda a finalização até 600 s.
- Sucesso, erro, desconexão e reset revogam a permissão; um cleanup antigo não revoga a geração nova. A sessão de negócio permanece ocupada após o upload. Somente o `unlock` do Kiosk registrado libera a sessão no fluxo WebSocket.
- O transporte também limita bytes enfileirados por socket e no conjunto dos sockets, inclusive em modo legado. Excesso encerra a conexão afetada.

`GET /health` mantém `ok` e acrescenta `uploads` com contadores de transferências ativas, pedidos/bytes em voo, pico e bytes atualmente no transporte. Não inclui tickets, conteúdo das fotos ou dados pessoais.

O registro do Kiosk e os endpoints administrativos preservam o modelo de autenticação anterior. A permissão nova protege o fluxo web dentro dessa fronteira; não constitui autenticação forte da instalação do Kiosk nem prova de presença física. O endurecimento dessas interfaces exige tratar o provisionamento de credenciais separadamente.

Publicar Bridge → Kiosk → Uploader, coordenando a troca dos clientes. Pares antigos continuam em v1; não são admitidos sobre um Kiosk v2. O uploader novo exige ACKs e não faz downgrade silencioso. Reverter somente o Bridge é incompatível com os clientes v2.

Contrato detalhado: `PicJoyKioskApp/api-documentation.md`. Resultados da etapa 2: `PicJoyKioskApp/docs/analisis/2026-11-12-etapa-2-entrega-confirmada.md`. A memória de todas as representações das imagens no Kiosk continua sendo tratada na etapa 3.
