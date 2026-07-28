CREATE TABLE IF NOT EXISTS predictions (
    product_id       text NOT NULL,
    as_of_ts         timestamptz NOT NULL,
    label_ts         timestamptz NOT NULL,
    pred_rv          double precision NOT NULL,
    pred_persistence double precision NOT NULL,
    model_version    text,
    features         jsonb,
    label_rv         double precision,
    labeled_at       timestamptz,
    created_at       timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (product_id, as_of_ts)
);
