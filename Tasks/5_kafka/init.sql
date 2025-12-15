-- Структура таблицы-источника + демо-данные
CREATE TABLE IF NOT EXISTS user_logins (
  id            SERIAL PRIMARY KEY,
  username      TEXT NOT NULL,
  event_type    TEXT NOT NULL,
  event_time    TIMESTAMP NOT NULL,
  sent_to_kafka BOOLEAN NOT NULL DEFAULT false
);

INSERT INTO user_logins (username, event_type, event_time)
VALUES
  ('alice', 'login',    now() - interval '5 minutes'),
  ('bob',   'purchase', now() - interval '3 minutes'),
  ('carol', 'register', now() - interval '1 minutes')
ON CONFLICT DO NOTHING;

CREATE INDEX IF NOT EXISTS idx_user_logins_sent ON user_logins(sent_to_kafka);
