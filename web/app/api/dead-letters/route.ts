import { NextResponse } from "next/server";
import { RELAY_URL } from "@/lib/relay";

export async function GET() {
  try {
    const res = await fetch(`${RELAY_URL}/dead-letters`, { cache: "no-store" });
    if (!res.ok) {
      return NextResponse.json(
        { error: `relay responded ${res.status}` },
        { status: 502 }
      );
    }
    const data = await res.json();
    return NextResponse.json(data);
  } catch (err) {
    return NextResponse.json(
      { error: `relay unreachable: ${(err as Error).message}` },
      { status: 502 }
    );
  }
}
