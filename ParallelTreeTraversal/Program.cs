using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.InteropServices;
using System.Security;
using System.Threading;
using System.Threading.Tasks;

namespace ParallelTreeTraversal {
    internal static class Processor {
        private static readonly ConcurrentBag<Tree> WorkBag = new ConcurrentBag<Tree>();

        public static void AddItem(Tree tree) {
            Console.WriteLine($"Queuing {tree.Name}");
            WorkBag.Add(tree);
        }

        public static Tree GetItem() {
            return WorkBag.TryTake(out var rc) ? rc : null;
        }

        public static void Process() {
            int retryCount = 0;
            while (true) {
                var workList = new List<Tree>();
                var t = GetItem();
                while (t != null) {
                    retryCount = 0;
                    workList.Add(t);
                    t = GetItem();
                }

                Console.WriteLine($"Processing {workList.Count} items");
                Parallel.ForEach(workList, x => x.Calculate());
                Thread.Sleep(100);
                retryCount++;
                if (retryCount == 10) {
                    break;
                }
            }
        }
    }

    internal class Tree {
        private int _queued = 0;

        public string Name { get; }
        public Tree Parent { get; set; }
        public List<Tree> Children { get; set; }
        public int Hash { get; set; }

        public Tree(string name) {
            Name = name;
        }

        public void AddChild(Tree child) {
            if (child == null) {
                return;
            }
            Children ??= new List<Tree>();
            Children.Add(child);
        }

        public void Visit() {
            if (Children?.Any() ?? false) {
                foreach (var child in Children) {
                    child.Visit();
                }
            }
            else {
                QueueWorkItemThreadSafe();
            }
        }

        public void QueueWorkItemThreadSafe() {
            if (Interlocked.CompareExchange(ref _queued, 1, 0) == 0) {
                Processor.AddItem(this);
            }
            else {
                // This is to protect agains a race condition.
                //
                // It can happend only when the last 2 or more children of
                // this node finish calculating at about the same time, and
                // both set their Hash to 1 before triggering a parent check.
                // In that case, the two parent checks would otherwise both
                // result in 'ready' and the parent would be queued twice.
                Console.WriteLine($"Parent {Name} already queued");
            }
        }

        [DllImport("Kernel32.dll"), SuppressUnmanagedCodeSecurity]
        public static extern int GetCurrentProcessorNumber();

        public void Calculate() {
            Console.WriteLine($"Calculating for {Name} on processor {GetCurrentProcessorNumber()}");
            Thread.Sleep(5000);
            Hash = 1;
            if (Parent?.IsReady() ?? false) {
                Console.WriteLine("Parent ready");
                Parent.QueueWorkItemThreadSafe();
            }
        }

        public bool IsReady() {
            Console.WriteLine($"Checking parent ready {Name}");
            return Children.All(x => x.Hash != 0);
        }
    }

    class Program {
        private static Random rnd = new Random();
        private static int nodeCount = 0;
        static void Main(string[] args) {
            var sw = new Stopwatch();
            Console.WriteLine("Hello World!");
            var tree = GenerateRandomTree();
            Console.WriteLine($"Tree of {nodeCount} nodes");
            sw.Start();
            var t = Task.Run(Processor.Process);
            tree.Visit();
            while (!t.IsCompleted) {
                var currentThreads = Process.GetCurrentProcess().Threads;
                Console.WriteLine($"Threads active {currentThreads.Count}");
                Thread.Sleep(1000);
            }
            t.Wait();
            sw.Stop();
            Console.WriteLine($"Elapsed {sw.ElapsedMilliseconds}");
        }

        static Tree GenerateRandomTree(int depth = 4, string name = "1") {
            if (depth == 0) {
                return null;
            }
            var rc = new Tree(name);
            nodeCount++;
            var nChildren = rnd.Next(9) + 1;
            for (int i = 0; i < nChildren; i++) {
                var child = GenerateRandomTree(depth - 1, $"{name}.{i + 1}");
                rc.AddChild(child);
                if (child != null) {
                    child.Parent = rc;
                }
            }

            return rc;
        }
    }
}
