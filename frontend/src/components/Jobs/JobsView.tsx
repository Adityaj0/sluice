import { Boxes, Code2, Workflow } from "lucide-react";
import { useState } from "react";

import { useStore } from "../../store/pipelineStore.ts";
import { Button, Card, EmptyState, Page, PageHeader, Segmented } from "../common/ui.tsx";
import { JobsTable } from "./JobsTable.tsx";

export function JobsView() {
  const { jobs, navigate } = useStore();
  const [filter, setFilter] = useState<"all" | "running" | "stopped">("all");
  const shown = jobs.filter((j) => filter === "all" || (filter === "running") === (j.status.state === "running"));
  return (
    <Page>
      <PageHeader
        title="Jobs"
        description="Every pipeline and SQL query deployed on this engine. Open one to watch it live."
        actions={
          <>
            <Button icon={Code2} onClick={() => navigate("sql")}>
              New SQL query
            </Button>
            <Button variant="primary" icon={Workflow} onClick={() => navigate("builder")}>
              New pipeline
            </Button>
          </>
        }
      />
      <Card
        title={`${shown.length} job${shown.length === 1 ? "" : "s"}`}
        bodyClassName=""
        actions={
          <Segmented
            value={filter}
            onChange={setFilter}
            options={[
              { id: "all", label: "All" },
              { id: "running", label: "Running" },
              { id: "stopped", label: "Stopped" },
            ]}
          />
        }
      >
        {shown.length ? (
          <JobsTable jobs={shown} />
        ) : (
          <EmptyState
            icon={Boxes}
            title={jobs.length ? "No jobs match this filter" : "No jobs yet"}
            action={
              !jobs.length && (
                <Button variant="primary" icon={Workflow} onClick={() => navigate("builder")}>
                  Build your first pipeline
                </Button>
              )
            }
          >
            {!jobs.length && "Jobs you deploy from the pipeline builder or the SQL editor appear here."}
          </EmptyState>
        )}
      </Card>
    </Page>
  );
}
