use axum::{extract::Request, middleware::Next, response::Response};
use std::convert::Infallible;

pub async fn strip_auth_middleware(mut request: Request, next: Next) -> Result<Response, Infallible> {
    request.headers_mut().remove("authorization");
    Ok(next.run(request).await)
}