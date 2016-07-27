package journal;

import org.helios.journal.util.AllocationMode;
import org.helios.journal.util.FilePreallocator;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.nio.file.Paths;

public class PreallocateTest
{
    private static final String JOURNAL_DIR = "./runtime";
    private static final int JOURNAL_COUNT = 2;

    private static final FilePreallocator allocator = new FilePreallocator(Paths.get(JOURNAL_DIR), JOURNAL_COUNT);

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
    public static void preallocateZeroed1GB()
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
