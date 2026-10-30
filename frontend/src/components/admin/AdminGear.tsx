import { Settings } from "lucide-react";
import { adminHref } from "../../lib/route";

/** Gear link into the admin area (#/admin/themes). Shared by the start screen and both console headers. */
export function AdminGear() {
  return (
    <a
      href={adminHref("themes")}
      aria-label="Settings: themes, architecture and insights"
      title="Settings"
      className="grid size-8 shrink-0 place-items-center rounded-[var(--radius-control)] border border-[var(--color-line)] text-[var(--color-muted)] transition-colors hover:text-[var(--color-text)]"
    >
      <Settings className="size-4" aria-hidden />
    </a>
  );
}
