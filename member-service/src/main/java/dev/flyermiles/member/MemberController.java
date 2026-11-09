package dev.flyermiles.member;

import dev.flyermiles.member.MemberRepository.Member;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/members")
public class MemberController {

  public record EnrolRequest(@NotBlank String name, @NotBlank @Email String email) {}

  public record BulkRequest(@Positive @Max(100_000) int count) {}

  private final MemberService members;
  private final MemberRepository repo;

  public MemberController(MemberService members, MemberRepository repo) {
    this.members = members;
    this.repo = repo;
  }

  @PostMapping
  @ResponseStatus(HttpStatus.CREATED)
  public Member enrol(@Valid @RequestBody EnrolRequest req) {
    return members.enrol(req.name(), req.email());
  }

  @PostMapping("/bulk")
  public Map<String, Integer> bulk(@Valid @RequestBody BulkRequest req) {
    return Map.of("created", members.enrolMany(req.count()));
  }

  @GetMapping("/{id}")
  public ResponseEntity<Member> get(@PathVariable UUID id) {
    return ResponseEntity.of(repo.find(id));
  }

  @GetMapping("/ids")
  public List<UUID> ids(@RequestParam(defaultValue = "10000") int limit) {
    return repo.ids(Math.min(limit, 100_000));
  }

  @PostMapping("/republish")
  public Map<String, Integer> republish() {
    return Map.of("published", members.republishAll());
  }
}
