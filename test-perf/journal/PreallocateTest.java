package journal;

import org.helios.core.journal.strategy.PositionalJournalling;
import org.helios.core.journal.util.AllocationMode;
import org.helios.core.journal.util.JournalAllocator;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;

public class PreallocateTest
{
    private static final JournalAllocator<FileChannel> allocator = new JournalAllocator<>(
        Paths.get("./runtime"), 1, PositionalJournalling.fileChannelFactory());

    public static void main(String[] args) throws Exception
    {
        Options opt = new OptionsBuilder()
            .include(PreallocateTest.class.getSimpleName())
            .shouldFailOnError(true)
            .build();

        new Runner(opt).run();
    }

    @Benchmark
    @Warmup(iterations = 5, time = 5)
    @Measurement(iterations = 10, time = 5)
    @BenchmarkMode(Mode.AverageTime)
    public static void preallocate()
    {
        try
        {
            allocator.preallocate(1024*1024*1024, AllocationMode.ZEROED_ALLOCATION);
        }
        catch (IOException e)
        {
        }
    }
}
