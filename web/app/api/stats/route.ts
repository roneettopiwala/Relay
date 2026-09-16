import { NextResponse } from "next/server";
import { RELAY_URL } from "@/lib/relay";

// Not cached (Route Handlers default to uncached for GET unless opted in —
// exactly right here, this is live data polled every few seconds).
export async function GET() {
  try {
    const res = await fetch(`${RELAY_URL}/stats`, { cache: "no-store" });
    if (!res.ok) {
      return NextResponse.json(
        { error: `relay responded ${res.status}` },
        { status: 502 }
      );
    }
    const data = await res.json();
    return NextResponse.json(data);
  } catch (err) {
    // Relay unreachable — a real, expected state while it's not running;
    // the dashboard should show "disconnected", not crash.
    return NextResponse.json(
      { error: `relay unreachable: ${(err as Error).message}` },
      { status: 502 }
    );
  }
}
