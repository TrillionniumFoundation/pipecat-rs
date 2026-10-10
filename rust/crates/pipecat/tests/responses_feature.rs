// Copyright (c) 2024-2026, Daily
// SPDX-License-Identifier: BSD-2-Clause
#![cfg(feature = "responses")]

use pipecat::responses::{
    InputItem, ResponsesConfig, ResponsesContext, ResponsesHttpClient, ResponsesRequest,
};
use pipecat::services::Role;

#[test]
fn optional_responses_api_is_available_without_opening_a_provider_connection() {
    let _client = ResponsesHttpClient::new(ResponsesConfig::new("unused-fixture-key")).unwrap();
    let _request = ResponsesRequest::new(
        "fixture-model",
        ResponsesContext::new(vec![InputItem::text(Role::User, "hello")]),
    );
}
