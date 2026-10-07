# Session current-schema fixtures

Session lifecycle tests create fresh encrypted current-schema databases through the public Session API. The current
message regression establishes and confirms a two-member group, reads its complete Welcome through public storage,
then checks cold reopen, complete membership/epoch/own-leaf equality, a real current application send, and complete
Welcome/sent-message record equality after another cold reopen. No historical message row is seeded or promoted.
Historical numbered account-schema database fixtures are retired. Independent wire/payload/snapshot encodings retain
their current contracts.
