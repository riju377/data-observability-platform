export default {
  async fetch(request) {
    // CORS headers to include in every response
    const corsHeaders = {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, Accept",
      "Access-Control-Max-Age": "86400", // 24 hours
    };

    // Handle preflight OPTIONS request
    if (request.method === "OPTIONS") {
      return new Response(null, {
        headers: corsHeaders
      });
    }

    try {
      const url = new URL(request.url);
      const target = url.searchParams.get("url");

      if (!target) {
        return new Response(
          JSON.stringify({ error: "Missing url param" }),
          {
            status: 400,
            headers: {
              "Content-Type": "application/json",
              ...corsHeaders
            }
          }
        );
      }

      console.log("Fetching:", target);

      const res = await fetch(target, {
        headers: {
          'User-Agent': 'Mozilla/5.0 (compatible; DataObservability/1.0)',
          'Accept': 'application/xml, application/json, text/plain, */*'
        }
      });

      console.log("Response status:", res.status);
      console.log("Response content-type:", res.headers.get("content-type"));

      if (!res.ok) {
        console.error("Target returned error:", res.status, res.statusText);
        return new Response(
          JSON.stringify({
            error: "Target URL returned error",
            status: res.status,
            statusText: res.statusText
          }),
          {
            status: res.status,
            headers: {
              "Content-Type": "application/json",
              ...corsHeaders
            }
          }
        );
      }

      // Get the response text first
      const text = await res.text();
      console.log("Response text length:", text.length);

      const contentType = res.headers.get("content-type") || "";

      // If it's XML, return it directly as text
      if (contentType.includes("xml")) {
        console.log("Returning XML response");
        return new Response(text, {
          headers: {
            "Content-Type": "text/xml",
            ...corsHeaders
          }
        });
      }

      // For JSON responses, parse and re-stringify
      if (contentType.includes("json") || text.trim().startsWith("{") || text.trim().startsWith("[")) {
        try {
          const data = JSON.parse(text);
          return new Response(JSON.stringify(data), {
            headers: {
              "Content-Type": "application/json",
              ...corsHeaders
            }
          });
        } catch (e) {
          console.error("Failed to parse JSON:", e.message);
          return new Response(
            JSON.stringify({
              error: "Target URL did not return valid JSON",
              responseText: text.substring(0, 500)
            }),
            {
              status: 502,
              headers: {
                "Content-Type": "application/json",
                ...corsHeaders
              }
            }
          );
        }
      }

      // For other content types, return as plain text
      return new Response(text, {
        headers: {
          "Content-Type": "text/plain",
          ...corsHeaders
        }
      });

    } catch (error) {
      console.error("Proxy error:", error);

      return new Response(
        JSON.stringify({
          error: "Failed to fetch target URL",
          message: error.message,
          stack: error.stack
        }),
        {
          status: 500,
          headers: {
            "Content-Type": "application/json",
            ...corsHeaders
          }
        }
      );
    }
  }
};
