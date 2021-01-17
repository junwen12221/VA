/**
 * Copyright [2021] [chen junwen]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.mycat.vertx.xa.log;

import cn.mycat.vertx.xa.State;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ImmutableCoordinatorLog implements Serializable {

    private final String xid;
    private final ImmutableParticipantLog[] participants;
    private final boolean commit;

    public ImmutableCoordinatorLog(String coordinatorId, ImmutableParticipantLog[] participants,boolean commit) {
        this.xid = coordinatorId;
        this.participants = participants;
        this.commit = commit;
    }
    public ImmutableCoordinatorLog(String coordinatorId, ImmutableParticipantLog[] participants) {
      this(coordinatorId,participants,false);
    }

    public String getXid() {
        return xid;
    }

    public List<ImmutableParticipantLog> getParticipants() {
        return Arrays.asList(participants);
    }

    public State computeMinState() {
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

    public boolean mayContains(State state) {
        if (commit){
            return State.XA_COMMITED == state;
        }
        return Arrays.stream(participants).anyMatch(immutableParticipantLog -> state ==  immutableParticipantLog.getState());
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

    public long computeExpires(){
       return Arrays.stream(participants).mapToLong(i->i.getExpires()).max().orElse(0);
    }

    public ImmutableCoordinatorLog withCommited(boolean success){
        return new ImmutableCoordinatorLog(xid,getParticipants().toArray(new ImmutableParticipantLog[0]),success);
    }
}
