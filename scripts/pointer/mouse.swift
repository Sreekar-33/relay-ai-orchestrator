// Smoothly glides the visible mouse cursor to (x, y) in screen points. No clicks, no permissions needed.
// usage: mouse glide <x> <y> [ms]   |   mouse pos
import CoreGraphics
import Foundation

func current() -> CGPoint { CGEvent(source: nil)?.location ?? .zero }
let args = CommandLine.arguments
if args.count >= 2 && args[1] == "pos" { let p = current(); print("\(Int(p.x)) \(Int(p.y))"); exit(0) }
guard args.count >= 4, args[1] == "glide", let tx = Double(args[2]), let ty = Double(args[3]) else {
    print("usage: mouse glide <x> <y> [ms] | mouse pos"); exit(1)
}
let ms = args.count >= 5 ? Double(args[4]) ?? 700 : 700
let start = current()
let steps = max(Int(ms / 12), 1)
for i in 1...steps {
    let t = Double(i) / Double(steps)
    let e = t < 0.5 ? 4 * t * t * t : 1 - pow(-2 * t + 2, 3) / 2          // ease-in-out cubic
    CGWarpMouseCursorPosition(CGPoint(x: start.x + (tx - start.x) * e, y: start.y + (ty - start.y) * e))
    CGAssociateMouseAndMouseCursorPosition(1)
    usleep(12_000)
}
