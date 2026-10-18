import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { cleanup, renderHook } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import type { ReactNode } from "react";

import { useProxyEventStream } from "./useProxyEventStream";

// 设置查询会走 Tauri IPC，测试里只关心端口这一个字段。
vi.mock("@/lib/query", () => ({
  useSettingsQuery: () => ({ data: { remotePort: 4200 } }),
}));

// 键值本身不是本用例的被测对象，但真实模块会拉进 api 层（jsdom 里没有 Tauri）。
vi.mock("@/lib/query/proxy", () => ({
  proxyKeys: {
    status: ["proxy", "status"],
    takeoverStatus: ["proxy", "takeoverStatus"],
  },
}));

class MockEventSource {
  static instances: MockEventSource[] = [];
  url: string;
  onmessage: ((event: MessageEvent) => void) | null = null;
  close = vi.fn();

  constructor(url: string) {
    this.url = url;
    MockEventSource.instances.push(this);
  }
}

function setup() {
  const client = new QueryClient();
  const invalidate = vi
    .spyOn(client, "invalidateQueries")
    .mockImplementation(async () => undefined);
  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={client}>{children}</QueryClientProvider>
  );
  const { unmount } = renderHook(() => useProxyEventStream(), { wrapper });
  return { invalidate, unmount };
}

function emit(es: MockEventSource, payload: unknown) {
  es.onmessage?.(
    new MessageEvent("message", { data: JSON.stringify(payload) }),
  );
}

describe("useProxyEventStream", () => {
  beforeEach(() => {
    MockEventSource.instances = [];
    vi.stubGlobal(
      "EventSource",
      MockEventSource as unknown as typeof EventSource,
    );
  });

  afterEach(() => {
    cleanup();
    vi.unstubAllGlobals();
  });

  it("订阅用户配置的远程端口，而不是写死 4000", () => {
    setup();
    expect(MockEventSource.instances[0]?.url).toBe(
      "http://127.0.0.1:4200/api/events",
    );
  });

  it("reorder 帧失效 providers 与 failoverQueue 两份缓存", () => {
    const { invalidate } = setup();
    const es = MockEventSource.instances[0]!;

    emit(es, { type: "reorder", degraded: true });

    expect(invalidate).toHaveBeenCalledWith({ queryKey: ["providers"] });
    expect(invalidate).toHaveBeenCalledWith({ queryKey: ["failoverQueue"] });
  });

  it("reorder 帧不误伤代理状态查询", () => {
    const { invalidate } = setup();
    const es = MockEventSource.instances[0]!;

    emit(es, { type: "reorder", degraded: true });

    expect(invalidate).not.toHaveBeenCalledWith({
      queryKey: ["proxy", "takeoverStatus"],
    });
  });

  it("非 JSON 帧不会抛错，也不会触发失效", () => {
    const { invalidate } = setup();
    const es = MockEventSource.instances[0]!;

    expect(() =>
      es.onmessage?.(new MessageEvent("message", { data: "not json" })),
    ).not.toThrow();
    expect(invalidate).not.toHaveBeenCalled();
  });
});
