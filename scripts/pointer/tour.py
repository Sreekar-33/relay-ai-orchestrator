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
  else if (kind === 'text') { el = byText(document.body, arg); }
  if (!el) return 'missing';
  if (phase === 'scroll') { el.scrollIntoView({block: 'center', behavior: 'smooth'}); return 'ok'; }
  const r = el.getBoundingClientRect();
  const z = window.outerWidth / window.innerWidth;
  const cx = Math.min(r.left + Math.min(r.width / 2, 60), window.innerWidth - 10);
  const x = window.screenX + cx * z;
  const y = window.screenY + (window.outerHeight - window.innerHeight * z) + (r.top + r.height / 2) * z;
  return JSON.stringify({x: Math.round(x), y: Math.round(y)});
})(%s, %s, %s)
"""


def run_js(kind, arg, phase):
    code = JS % (json.dumps(kind), json.dumps(arg), json.dumps(phase))
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


def main():
    for action in sys.argv[1:]:
        kind, _, arg = action.partition(":")
        if kind == "wait":
            time.sleep(float(arg or 1)); continue
        if kind in ("top", "scroll"):
            run_js(kind, arg, "act"); time.sleep(0.9); continue
        if point(kind, arg):
            if kind == "open":
                time.sleep(0.4); run_js(kind, arg, "act")
            time.sleep(1.3)  # dwell so the viewer can follow


if __name__ == "__main__":
    main()
