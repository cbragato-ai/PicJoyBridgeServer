# PicJoyBridgeServer

Bridge HTTP/WebSocket entre PicJoyUploader e PicJoyKioskApp.

- Desenvolvimento: `npm run dev`.
- Compilação TypeScript: `npm run build`.
- Testes de upload: `npm run test:upload` (inclui WebSockets locais; não usa banco ou serviços reais).

## Contenção do upload — protocolo 1

O cliente pode enviar `protocolVersion: 1` no `register`. A resposta `registered` confirma a versão e informa `uploadLimits`: `maxFiles: 100`, `maxFileBytes: 20971520`, `maxTotalBytes: 2147483648`, `maxChunkChars: 65536`.

Para web registrado nessa versão, o Bridge contabiliza o lote por conexão, sem guardar conteúdo das fotos. Valida `file-meta` (`fileName`, `size`, `totalBase64Length`), `file-chunk` (`fileName`, `seq`, `chunkBase64`) e `file-complete`. Os nomes precisam ser únicos; sequências começam em zero e são contíguas. A finalização exige o tamanho Base64 declarado. `notify` com `message: 'transfer_complete'` exige todos os arquivos completos. Repetir o registro do mesmo socket não reinicia os contadores. Só uma nova conexão inicia outro lote; retomada e idempotência ficam para a etapa 2.

Em rejeições, envia `transfer-error` com `protocolVersion`, `code` e `message` ao web e ao Kiosk da sessão, e fecha somente o web com código 1008. Um erro enviado pelo Kiosk é encaminhado ao web da mesma sessão como `KIOSK_UPLOAD_ERROR`. Não é um ACK de entrega. Frames WebSocket acima de 128 KiB são recusados pelo transporte, antes do parse JSON, e podem encerrar a conexão sem uma mensagem de erro da aplicação.

Clientes sem versão mantêm o encaminhamento legado. Não recebem a garantia dos limites de lote no Bridge; um Kiosk atualizado também valida o recebimento. Versões desconhecidas são recusadas. Publicar primeiro Bridge, depois Kiosk e por último Uploader.

Existe um único registro por socket. Erros de conexão ou de `send()` removem e terminam o socket afetado, com limpeza idempotente. Esta etapa não implementa ACKs, fila de saída limitada, retomada, autenticação completa da transferência ou armazenamento de fotos em disco. Não comprova capacidade de 100 fotos em produção.
