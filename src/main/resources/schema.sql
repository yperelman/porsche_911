CREATE TABLE IF NOT EXISTS attributed_page_views (
    page_view_id            TEXT NOT NULL,
    user_id                 TEXT NOT NULL,
    event_time              TIMESTAMPTZ NOT NULL,
    url                     TEXT NOT NULL,
    attributed_campaign_id  TEXT,
    attributed_click_id     TEXT,
    attributed_click_time   TIMESTAMPTZ,
    written_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- page_view_id is not globally unique (ids can repeat across users), so identity
    -- is (page_view_id, user_id). This is also the page-view dedup key.
    PRIMARY KEY (page_view_id, user_id)
);

-- Correction UPDATEs locate rows by (user_id, event_time).
CREATE INDEX IF NOT EXISTS idx_apv_user_event_time ON attributed_page_views (user_id, event_time);
