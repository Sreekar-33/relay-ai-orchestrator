#!/usr/bin/env python3
"""Glide the real mouse cursor over console elements (hover only; it never clicks).
Needs Chrome > View > Developer > Allow JavaScript from Apple Events.
usage: tour.py ACTION [ACTION ...]
  step:<node_id>     point at a step's name       badge:<node_id>   its status badge
  pills:<node_id>    its pills (tokens/gate/branch) open:<node_id>   point at its details line and expand it
  inside:<node_id>:<text>  point at text inside that step's (expanded) details
  sel:<css>          first element matching CSS    text:<words>      smallest visible element containing words
  scroll:<px>        smooth scroll by px           top               scroll to top        wait:<sec>
"""
import json
import os
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
MOUSE = os.path.join(HERE, "mouse")

JS = r"""
(function(kind, arg, phase){
  const steps = [...document.querySelectorAll('li.step')];
  const stepOf = (id) => steps.find(li => (li.querySelector('.nid')||{}).textContent?.trim() === id);
  const visible = (el) => { const r = el.getBoundingClientRect(); return r.width > 0 && r.height > 0; };
  const byText = (root, words) => {
    let best = null;
    for (const el of root.querySelectorAll('*')) {
      if (!visible(el) || !el.textContent.includes(words)) continue;
      if (!best || el.textContent.length < best.textContent.length) best = el;
    }
    return best;
  };
  let el = null;
  if (kind === 'top') { window.scrollTo({top: 0, behavior: 'smooth'}); return 'ok'; }
  if (kind === 'scroll') { window.scrollBy({top: Number(arg), behavior: 'smooth'}); return 'ok'; }
  if (kind === 'step') { const s = stepOf(arg); el = s && s.querySelector('.nid'); }
  else if (kind === 'badge') { const s = stepOf(arg); el = s && s.querySelector('.step-head .badge'); }
  else if (kind === 'pills') { const s = stepOf(arg); el = s && (s.querySelector('.pills > *') || s.querySelector('.pills')); }
  else if (kind === 'open') { const s = stepOf(arg); el = s && s.querySelector('details > summary'); if (el && phase === 'act') { el.parentElement.open = true; return 'ok'; } }
  else if (kind === 'inside') { const [id, ...w] = arg.split(':'); const s = stepOf(id); el = s && byText(s, w.join(':')); }
  else if (kind === 'sel') { el = document.querySelector(arg); }
  else if (kind === 'href') { el = [...document.querySelectorAll('a[href]')].find(a => a.getAttribute('href') === arg && visible(a)); }
  else if (kind === 'nav') { el = document.querySelector('#nav a[data-nav="' + arg + '"]'); }
  else if (kind === 'where') { return location.pathname.startsWith('/console') ? (location.hash || '#/') : 'other'; }
  else if (kind === 'text') { el = byText(document.body, arg); }
  if (!el) return 'missing';
  if (phase === 'scroll') { el.scrollIntoView({block: 'center', behavior: 'smooth'}); return 'ok'; }
  if (phase === 'click') {
    const rr = el.getBoundingClientRect(), cx = rr.left + Math.min(rr.width / 2, 60), cy = rr.top + rr.height / 2;
    const d = document.createElement('div');
    d.style.cssText = `position:fixed;left:${cx-14}px;top:${cy-14}px;width:28px;height:28px;border-radius:50%;background:rgba(59,130,246,.35);border:2px solid rgba(59,130,246,.95);pointer-events:none;z-index:99999;transition:transform .5s ease,opacity .5s ease`;
    document.body.appendChild(d);
    requestAnimationFrame(() => { d.style.transform = 'scale(2.4)'; d.style.opacity = '0'; });
    setTimeout(() => d.remove(), 650);
    setTimeout(() => el.click(), 120);
    return 'ok';
  }
  const r = el.getBoundingClientRect();
  const z = window.outerWidth / window.innerWidth;
  const cx = Math.min(r.left + Math.min(r.width / 2, 60), window.innerWidth - 10);
  const x = window.screenX + cx * z;
  const y = window.screenY + (window.outerHeight - window.innerHeight * z) + (r.top + r.height / 2) * z;
  return JSON.stringify({x: Math.round(x), y: Math.round(y)});
})(__KIND__, __ARG__, __PHASE__)
"""


def run_js(kind, arg, phase):
    code = JS.replace("__KIND__", json.dumps(kind)).replace("__ARG__", json.dumps(arg)).replace("__PHASE__", json.dumps(phase))
    with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False) as f:
        f.write(code)
        path = f.name
    try:
        out = subprocess.run(["osascript", "-e", f'set js to read POSIX file "{path}" as «class utf8»',
                              "-e", 'tell application "Google Chrome" to execute front window\'s active tab javascript js'],
                             capture_output=True, text=True, timeout=5)
        return out.stdout.strip() or out.stderr.strip()
    finally:
        os.unlink(path)


def point(kind, arg, ms=800):
    for _ in range(12):  # page may still be rendering
        if run_js(kind, arg, "scroll") == "ok":
            break
        time.sleep(0.25)
    else:
        return False
    time.sleep(0.6)  # let the smooth scroll settle
    res = run_js(kind, arg, "rect")
    try:
        p = json.loads(res)
    except (json.JSONDecodeError, TypeError):
        return False
    subprocess.run([MOUSE, "glide", str(p["x"]), str(p["y"]), str(ms)])
    return True


def click(kind, arg):
    """Glide to the element, show a click ripple, click it."""
    if not point(kind, arg, ms=650):
        return False
    time.sleep(0.25)
    ok = run_js(kind, arg, "click") == "ok"
    time.sleep(1.0)
    return ok


def wait_for(kind, arg, secs=8.0):
    end = time.time() + secs
    while time.time() < end:
        if run_js(kind, arg, "scroll") == "ok":
            return True
        time.sleep(0.4)
    return False


def goto(path):
    """Navigate the console to #<path> by clicking through the UI. Returns False if not possible (caller falls back)."""
    where = run_js("where", "", "rect")
    if where == "other" or not where.startswith("#"):
        return False
    href = "#" + path
    if where == href:
        return True
    if run_js("href", href, "scroll") == "ok":                 # a link to it is already on screen (e.g. dashboard row)
        return click("href", href)
    if path.startswith("/runs/"):
        if where != "#/runs":
            click("nav", "/runs")
        if wait_for("href", href, 9):
            return click("href", href)
        return False
    top = path if path in ("/workflows", "/runs", "/approvals", "/world", "/build") else "/"
    return click("nav", top)


def main():
    if len(sys.argv) == 3 and sys.argv[1] == "goto":
        sys.exit(0 if goto(sys.argv[2]) else 1)
    for action in sys.argv[1:]:
        kind, _, arg = action.partition(":")
        if kind == "wait":
            time.sleep(float(arg or 1)); continue
        if kind == "click":
            sub, _, rest = arg.partition(":")
            click(sub, rest); continue
        if kind in ("top", "scroll"):
            run_js(kind, arg, "act"); time.sleep(0.9); continue
        if point(kind, arg):
            if kind == "open":
                time.sleep(0.4); run_js(kind, arg, "act")
            time.sleep(1.3)  # dwell so the viewer can follow


if __name__ == "__main__":
    main()
