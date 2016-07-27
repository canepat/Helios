package org.helios.journal;

import org.agrona.BitUtil;

public abstract class JournalRecordDescriptor
{
    public static final int MESSAGE_HEAD  = 0xC0DEC0DE;
    public static final int MESSAGE_TRAIL = 0xED0CED0C;

    public static final int MESSAGE_HEAD_SIZE     = BitUtil.SIZE_OF_INT;
    public static final int MESSAGE_TYPE_SIZE     = BitUtil.SIZE_OF_INT;
    public static final int MESSAGE_LENGTH_SIZE   = BitUtil.SIZE_OF_INT;
    public static final int MESSAGE_TRAIL_SIZE    = BitUtil.SIZE_OF_INT;
    public static final int MESSAGE_CHECKSUM_SIZE = BitUtil.SIZE_OF_INT;

    public static final int MESSAGE_HEADER_SIZE  = MESSAGE_HEAD_SIZE + MESSAGE_TYPE_SIZE + MESSAGE_LENGTH_SIZE;
    public static final int MESSAGE_TRAILER_SIZE = MESSAGE_TRAIL_SIZE + MESSAGE_CHECKSUM_SIZE;
    public static final int MESSAGE_FRAME_SIZE   = MESSAGE_HEADER_SIZE + MESSAGE_TRAILER_SIZE;
}
