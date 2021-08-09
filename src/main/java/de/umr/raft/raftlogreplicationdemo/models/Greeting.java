package de.umr.raft.raftlogreplicationdemo.models;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor()
public class Greeting {

    @Getter final long id;
    @Getter @NonNull final String content;
}
