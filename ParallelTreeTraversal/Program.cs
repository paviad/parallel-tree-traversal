﻿using System;
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
        public static int[] ProcessorWorkCount = new int[20];
        public static int[] ProcessorTimeCount = new int[20];

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
                Parallel.ForEach(workList, x => {
                    var processor = x.Calculate();
                    x.QueueParentIfReady();
                    Interlocked.Increment(ref ProcessorWorkCount[processor]);
                    Interlocked.Add(ref ProcessorTimeCount[processor], (int)x.Elapsed);
                });
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
        public long Elapsed { get; private set; }

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

        public int Calculate() {
            var sw = new Stopwatch();
            sw.Start();
            var currentProcessorNumber = GetCurrentProcessorNumber();
            Console.WriteLine($"Calculating for {Name} on processor {currentProcessorNumber}");
            uint n;
            int j = 0;
            string randomFileName;
            do {
                // randomFileName = System.IO.Path.GetRandomFileName();
                randomFileName = $"${Name}-{j++}";
                n = (uint)randomFileName.GetHashCode();
            } while (n > 500);

            sw.Stop();
            Elapsed = sw.ElapsedMilliseconds;
            Console.WriteLine($"Found {randomFileName} with hash {n} after {Elapsed} msec (processor {currentProcessorNumber})");
            Hash = 1;

            return currentProcessorNumber;
        }

        public void QueueParentIfReady() {
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
        private static readonly Random Rnd = new Random();
        private static int _nodeCount = 0;
        private static readonly List<Tree> AllNodes = new List<Tree>();

        static void Main(string[] args) {
            Console.WriteLine("Hello World!");
            var tree = GenerateRandomTree(3);
            Console.WriteLine($"Tree of {_nodeCount} nodes");

            Console.WriteLine("--------Serial Run--------");
            RunSerial();
            Console.WriteLine("-------Parallel Run-------");
            RunParallel(tree);
        }

        private static void RunParallel(Tree tree) {
            var sw = new Stopwatch();
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
            var sumElapsed = AllNodes.Sum(x => x.Elapsed);
            var elapsed = sw.ElapsedMilliseconds;
            Console.WriteLine(
                $"Elapsed {elapsed}, for total calculation time of {sumElapsed} (parallelism ratio: {1.0 * sumElapsed / elapsed})");
            for (int i = 0; i < Processor.ProcessorWorkCount.Length; i++) {
                var n = Processor.ProcessorWorkCount[i];
                var ptime = Processor.ProcessorTimeCount[i];
                Console.WriteLine($"Ran {n} jobs on processor {i} for {ptime} msec");
            }

            sw.Reset();
        }

        private static void RunSerial() {
            var sw = new Stopwatch();
            sw.Start();
            foreach (var node in AllNodes) {
                Console.WriteLine($"{node.Name}");
                node.Calculate();
            }

            sw.Stop();
            Console.WriteLine($"Serial time {sw.ElapsedMilliseconds} msec");
            sw.Reset();
        }

        static Tree GenerateRandomTree(int depth, string name = "1") {
            if (depth == 0) {
                return null;
            }
            var rc = new Tree(name);
            _nodeCount++;
            AllNodes.Add(rc);
            var nChildren = Rnd.Next(9) + 1;
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
