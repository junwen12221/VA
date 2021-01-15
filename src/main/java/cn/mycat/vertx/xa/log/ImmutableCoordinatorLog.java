
package cn.mycat.vertx.xa.log;

import cn.mycat.vertx.xa.State;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ImmutableCoordinatorLog implements Serializable {

    private final String xid;
    private final ImmutableParticipantLog[] participants;

    public ImmutableCoordinatorLog(String coordinatorId, ImmutableParticipantLog[] participants) {
        this.xid = coordinatorId;
        this.participants = participants;
    }

    public String getXid() {
        return xid;
    }

    public List<ImmutableParticipantLog> getParticipants() {
        return Arrays.asList(participants);
    }

    public State computeState() {
        State txState = State.XA_COMMITED;
        for (ImmutableParticipantLog participant : participants) {
            if (txState == State.XA_INITED) {
                return State.XA_INITED;
            }
            if (txState.compareTo(participant.getState()) > 0) {
                txState = participant.getState();

            }
        }
        return txState;
    }
    public ImmutableParticipantLog[] replace(String target, State state, long expires) {
        ArrayList<ImmutableParticipantLog> res = new ArrayList<>();
        boolean find = false;
        for (ImmutableParticipantLog participant : participants) {
            if (participant.getTarget().equals(target)) {
                find = true;
                res.add(participant.copy(state));
            } else {
                res.add(participant);
            }
        }
        if (!find) {
            res.add(new ImmutableParticipantLog(target, expires, state));
        }
        return res.toArray(new ImmutableParticipantLog[]{});
    }
}
