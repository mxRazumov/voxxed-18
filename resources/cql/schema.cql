CREATE TABLE subject_idx (
    partition text,
    sender text,
    word text,
    message_id text,
    PRIMARY KEY ((partition, sender), word, message_id));